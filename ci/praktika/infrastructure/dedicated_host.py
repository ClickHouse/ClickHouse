import json
from dataclasses import dataclass, field
from typing import Any, Dict, List


class DedicatedHost:

    @dataclass
    class Config:
        # Logical pool name (used for tagging/identification)
        name: str
        region: str = ""

        # Mandatory runner identification fields
        praktika_resource_tag: str = (
            ""  # Praktika resource tag (e.g., "mac") - tagged as "praktika"
        )

        # If set, allocate hosts in all availability zones in the region.
        all_availability_zones: bool = False
        availability_zones: List[str] = field(default_factory=list)

        # EC2 instance type for host allocation (mac hosts)
        instance_type: str = ""

        # Auto placement for Dedicated Hosts ("on" or "off")
        # IMPORTANT: Must be set to "on" for EC2 instances with tenancy="host" to be
        # automatically placed on these hosts. With auto_placement="on", AWS automatically
        # places instances that match the instance type and availability zone onto available
        # hosts in this pool. EC2Instance.Config does not need to specify host_resource_group_name
        # when auto_placement is enabled - AWS handles placement automatically.
        #
        # TODO: For more complex scenarios requiring explicit host targeting (multiple host pools,
        # specific placement policies), implement ResourceGroup.Config or enhance EC2Instance.Config
        # to support explicit host_resource_group_name targeting.
        auto_placement: str = "off"

        # Desired host count per AZ
        quantity_per_az: int = 1

        # Tags applied to allocated hosts
        tags: Dict[str, str] = field(default_factory=dict)

        # If set (or defaulted), a Resource Group will be created/updated to select hosts in this pool.
        # This is useful for launching instances with Placement.HostResourceGroupArn.
        host_resource_group_name: str = ""

        # Extra fetched/derived properties
        ext: Dict[str, Any] = field(default_factory=dict)

        def _resolved_availability_zones(self) -> List[str]:
            if self.availability_zones:
                return self.availability_zones
            if not self.all_availability_zones:
                raise ValueError(
                    f"Either availability_zones must be set or all_availability_zones=True for DedicatedHost '{self.name}'"
                )

            import boto3

            ec2 = boto3.client("ec2", region_name=self.region)
            resp = ec2.describe_availability_zones(
                Filters=[{"Name": "region-name", "Values": [self.region]}]
            )
            zones = [
                z["ZoneName"]
                for z in resp.get("AvailabilityZones", [])
                if z.get("State") == "available" and z.get("ZoneName")
            ]
            if not zones:
                raise Exception(f"No available AZs found for region '{self.region}'")
            return zones

        def _host_filters(self, az: str) -> List[Dict[str, Any]]:
            filters: List[Dict[str, Any]] = [
                {"Name": "availability-zone", "Values": [az]},
            ]

            if self.instance_type:
                filters.append(
                    {"Name": "instance-type", "Values": [self.instance_type]}
                )

            # Stable identification tags
            merged_tags = {"praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged_tags["praktika_resource_tag"] = self.praktika_resource_tag
            # Add user-defined tags
            merged_tags.update(self.tags or {})

            for k, v in merged_tags.items():
                filters.append({"Name": f"tag:{k}", "Values": [v]})

            return filters

        def fetch(self):
            import boto3

            ec2 = boto3.client("ec2", region_name=self.region)

            # self._ensure_host_resource_group()

            azs = self._resolved_availability_zones()
            hosts_by_az: Dict[str, List[str]] = {}
            releasing_hosts_by_az: Dict[str, List[str]] = {}

            for az in azs:
                resp = ec2.describe_hosts(Filters=self._host_filters(az))
                hosts = resp.get("Hosts", [])
                # Only count hosts in available/allocated states
                host_ids = [
                    h.get("HostId")
                    for h in hosts
                    if h.get("HostId")
                    and h.get("State")
                    in ["available", "under-assessment", "permanent-failure"]
                ]
                # Track hosts being released
                releasing_ids = [
                    h.get("HostId")
                    for h in hosts
                    if h.get("HostId")
                    and h.get("State") == "released-permanent-failure"
                ]
                hosts_by_az[az] = host_ids
                releasing_hosts_by_az[az] = releasing_ids

            self.ext["hosts_by_az"] = hosts_by_az
            self.ext["releasing_hosts_by_az"] = releasing_hosts_by_az
            print(f"Successfully fetched Dedicated Hosts for pool: {self.name}")
            return self

        def _ensure_host_resource_group(self):
            import boto3

            group_name = self.host_resource_group_name or self.name
            self.host_resource_group_name = group_name

            merged_tags = {"praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged_tags["praktika_resource_tag"] = self.praktika_resource_tag
            # Add user-defined tags
            merged_tags.update(self.tags or {})

            query_obj = {
                "ResourceTypeFilters": ["AWS::EC2::Host"],
                "TagFilters": [
                    {"Key": k, "Values": [v]} for k, v in merged_tags.items()
                ],
            }
            desired_query = {
                "Type": "TAG_FILTERS_1_0",
                "Query": json.dumps(query_obj),
            }

            rg = boto3.client("resource-groups", region_name=self.region)

            exists = False
            try:
                resp = rg.get_group(GroupName=group_name)
                group = resp.get("Group") or {}
                arn = group.get("GroupArn")
                if arn:
                    self.ext["host_resource_group_arn"] = arn
                exists = True
            except Exception:
                exists = False

            if not exists:
                resp = rg.create_group(
                    Name=group_name,
                    ResourceQuery=desired_query,
                    Description=f"Praktika Dedicated Host pool {self.name}",
                )
                group = resp.get("Group") or {}
                arn = group.get("GroupArn")
                if arn:
                    self.ext["host_resource_group_arn"] = arn
                print(
                    f"Created Resource Group '{group_name}' for DedicatedHost pool '{self.name}'"
                )
                return self

            rg.update_group_query(
                GroupName=group_name,
                ResourceQuery=desired_query,
            )
            print(
                f"Updated Resource Group '{group_name}' query for DedicatedHost pool '{self.name}'"
            )
            return self

        def deploy(self):
            import boto3

            if not self.instance_type:
                raise ValueError(
                    f"instance_type must be set for DedicatedHost '{self.name}' (e.g. mac2-m2.metal)"
                )
            if self.quantity_per_az < 1:
                raise ValueError(
                    f"quantity_per_az must be >= 1 for DedicatedHost '{self.name}'"
                )

            # IMPORTANT: auto_placement must be "on" for EC2 instances to use these hosts
            # Without auto_placement="on", instances with tenancy="host" will not be
            # automatically placed on these dedicated hosts.
            if self.auto_placement != "on":
                raise ValueError(
                    f"auto_placement must be 'on' for DedicatedHost '{self.name}'. "
                    f"This is required for EC2 instances with tenancy='host' to be automatically "
                    f"placed on these dedicated hosts. Currently set to: '{self.auto_placement}'"
                )

            ec2 = boto3.client("ec2", region_name=self.region)

            azs = self._resolved_availability_zones()

            # Make allocation idempotent-ish by counting existing tagged hosts
            self.fetch()
            hosts_by_az: Dict[str, List[str]] = self.ext.get("hosts_by_az", {})

            allocated_by_az: Dict[str, List[str]] = {}

            merged_tags = {"praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged_tags["praktika_resource_tag"] = self.praktika_resource_tag
            # Add user-defined tags
            merged_tags.update(self.tags or {})

            for az in azs:
                existing = hosts_by_az.get(az, [])
                releasing = self.ext.get("releasing_hosts_by_az", {}).get(az, [])

                # Warn if hosts are being released - this can cause allocation failures
                if releasing:
                    print(
                        f"Warning: {len(releasing)} host(s) are being released in {az}. "
                        f"This may cause InsufficientHostCapacity errors until release completes. "
                        f"Releasing hosts: {releasing}"
                    )

                # Enforce desired auto placement on existing hosts
                if existing:
                    try:
                        ec2.modify_hosts(
                            HostIds=existing, AutoPlacement=self.auto_placement
                        )
                    except Exception as e:
                        print(
                            f"Warning: Failed to set AutoPlacement={self.auto_placement} for existing hosts in {az}: {e}"
                        )

                missing = self.quantity_per_az - len(existing)
                if missing <= 0:
                    print(
                        f"DedicatedHost pool '{self.name}': AZ {az} already has {len(existing)} host(s), need {self.quantity_per_az} - skip"
                    )
                    allocated_by_az[az] = []
                    continue

                print(
                    f"Allocating {missing} Dedicated Host(s) for pool '{self.name}' in {az} (instance_type={self.instance_type})"
                )

                try:
                    from botocore.exceptions import ClientError

                    resp = ec2.allocate_hosts(
                        AvailabilityZone=az,
                        InstanceType=self.instance_type,
                        Quantity=missing,
                        AutoPlacement=self.auto_placement,
                        TagSpecifications=[
                            {
                                "ResourceType": "dedicated-host",
                                "Tags": [
                                    {"Key": k, "Value": v}
                                    for k, v in merged_tags.items()
                                ],
                            }
                        ],
                    )

                    new_ids = resp.get("HostIds", [])
                    allocated_by_az[az] = new_ids
                    print(
                        f"Allocated {len(new_ids)} Dedicated Host(s) in {az} for pool '{self.name}': {new_ids}"
                    )
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "InsufficientHostCapacity":
                        releasing_msg = ""
                        if releasing:
                            releasing_msg = f" Note: {len(releasing)} host(s) are currently being released - wait for release to complete."
                        print(
                            f"Warning: Insufficient capacity to allocate {missing} host(s) in {az} "
                            f"for pool '{self.name}' (instance_type={self.instance_type}) - skipping this AZ. "
                            f"This is common for Mac dedicated hosts.{releasing_msg} "
                            f"Try again later or contact AWS support."
                        )
                        allocated_by_az[az] = []
                    elif error_code == "HostLimitExceeded":
                        # Likely caused by existing hosts that are not tagged with
                        # the expected praktika tags and therefore not found by fetch().
                        # List all hosts of this instance type in the AZ to help diagnose.
                        try:
                            all_resp = ec2.describe_hosts(
                                Filters=[
                                    {"Name": "availability-zone", "Values": [az]},
                                    {"Name": "instance-type", "Values": [self.instance_type]},
                                    {"Name": "state", "Values": ["available", "under-assessment"]},
                                ]
                            )
                            all_host_ids = [
                                h.get("HostId")
                                for h in all_resp.get("Hosts", [])
                                if h.get("HostId")
                            ]
                            untagged = [h for h in all_host_ids if h not in existing]
                        except Exception:
                            all_host_ids = []
                            untagged = []
                        hint = ""
                        if untagged:
                            hint = (
                                f" Found {len(untagged)} existing host(s) in {az} not matched by tags"
                                f" (missing 'praktika_rn={self.name}' or"
                                f" 'praktika_resource_tag={self.praktika_resource_tag}'): {untagged}."
                                f" Tag them or increase quantity_per_az to account for them."
                            )
                        print(
                            f"Warning: Host limit exceeded when trying to allocate {missing} host(s)"
                            f" in {az} for pool '{self.name}'.{hint}"
                        )
                        allocated_by_az[az] = []
                    elif error_code == "UnsupportedHostConfiguration":
                        print(
                            f"Warning: Instance type '{self.instance_type}' is not supported in {az} - skipping this AZ. "
                            f"Mac dedicated hosts are only available in specific AZs (typically us-east-1a, us-west-2a, etc.). "
                            f"Update your configuration to use supported AZs only."
                        )
                        allocated_by_az[az] = []
                    else:
                        raise

            self.ext["allocated_by_az"] = allocated_by_az
            return self

        def shutdown(self, force: bool = True):
            """
            Release all Dedicated Hosts in this pool.

            Args:
                force: Not used for DedicatedHost (kept for API consistency with EC2Instance).
            """
            import boto3

            _ = force  # Unused, kept for API consistency

            ec2 = boto3.client("ec2", region_name=self.region)

            # Fetch existing hosts
            self.fetch()
            hosts_by_az: Dict[str, List[str]] = self.ext.get("hosts_by_az", {})

            if not hosts_by_az or not any(hosts_by_az.values()):
                print(
                    f"DedicatedHost pool '{self.name}': no hosts found - nothing to shutdown"
                )
                return self

            released_count = 0
            failed_count = 0

            for az, host_ids in hosts_by_az.items():
                if not host_ids:
                    continue

                print(
                    f"Releasing {len(host_ids)} Dedicated Host(s) in {az} for pool '{self.name}'"
                )

                for host_id in host_ids:
                    try:
                        ec2.release_hosts(HostIds=[host_id])
                        print(f"  Released host: {host_id}")
                        released_count += 1
                    except Exception as e:
                        print(f"  Failed to release host {host_id}: {e}")
                        failed_count += 1

            print(
                f"DedicatedHost pool '{self.name}': released {released_count} host(s), "
                f"failed {failed_count}"
            )
            return self
