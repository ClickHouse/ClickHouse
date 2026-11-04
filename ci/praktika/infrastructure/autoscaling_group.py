import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


class AutoScalingGroup:

    @dataclass
    class Config:
        # ASG name
        name: str
        region: str = ""

        # Mandatory runner identification fields
        praktika_resource_tag: str = (
            ""  # Praktika resource tag (e.g., "mac") - tagged as "praktika_resource_tag"
        )
        runner_type: str = (
            ""  # GitHub runner type (e.g., "arm_macos_small") - tagged as "github:runner-type"
        )

        # Networking
        subnet_ids: List[str] = field(default_factory=list)
        vpc_id: str = ""
        vpc_name: str = ""  # VPC Name tag value
        availability_zones: List[str] = field(default_factory=list)

        # Capacity
        min_size: int = 0
        max_size: int = 0
        desired_capacity: Optional[int] = None

        # Health checks
        health_check_type: str = "EC2"  # EC2 | ELB
        health_check_grace_period_sec: int = 0

        # Launch template
        launch_template_id: str = ""
        launch_template_name: str = ""
        launch_template_version: str = "$Latest"

        # Load balancing (optional)
        target_group_arns: List[str] = field(default_factory=list)

        # Tags applied to instances (propagate_at_launch=True)
        tags: Dict[str, str] = field(default_factory=dict)

        # Extra fetched/derived properties
        ext: Dict[str, Any] = field(default_factory=dict)

        def fetch(self):
            """
            Fetch Auto Scaling Group configuration from AWS and store in ext dictionary.

            Raises:
                Exception: If ASG does not exist or AWS API call fails
            """
            import boto3

            asg_client = boto3.client("autoscaling", region_name=self.region)

            resp = asg_client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[self.name]
            )
            groups = resp.get("AutoScalingGroups", [])
            if not groups:
                raise Exception(f"Auto Scaling group '{self.name}' not found in AWS")

            group = groups[0]

            self.ext["auto_scaling_group_arn"] = group.get("AutoScalingGroupARN")
            self.ext["min_size"] = group.get("MinSize")
            self.ext["max_size"] = group.get("MaxSize")
            self.ext["desired_capacity"] = group.get("DesiredCapacity")
            self.ext["default_cooldown"] = group.get("DefaultCooldown")
            self.ext["health_check_type"] = group.get("HealthCheckType")
            self.ext["health_check_grace_period"] = group.get("HealthCheckGracePeriod")
            self.ext["vpc_zone_identifier"] = group.get("VPCZoneIdentifier", "")
            self.ext["availability_zones"] = group.get("AvailabilityZones", [])
            self.ext["target_group_arns"] = group.get("TargetGroupARNs", [])
            self.ext["load_balancer_names"] = group.get("LoadBalancerNames", [])
            self.ext["created_time"] = group.get("CreatedTime")
            self.ext["instances"] = group.get("Instances", [])

            lt = group.get("LaunchTemplate")
            if lt:
                self.ext["launch_template"] = {
                    "id": lt.get("LaunchTemplateId"),
                    "name": lt.get("LaunchTemplateName"),
                    "version": lt.get("Version"),
                }

            fetched_tags = {}
            for t in group.get("Tags", []) or []:
                if t.get("PropagateAtLaunch") and t.get("Key"):
                    fetched_tags[t["Key"]] = t.get("Value", "")
            self.ext["tags"] = fetched_tags

            print(f"Successfully fetched configuration for ASG: {self.name}")
            return self

        def _build_launch_template_spec(self) -> Dict[str, str]:
            version = self.launch_template_version
            if version in ("$Latest", "$Default"):
                import boto3

                ec2 = boto3.client("ec2", region_name=self.region)
                if self.launch_template_id:
                    lt_resp = ec2.describe_launch_templates(
                        LaunchTemplateIds=[self.launch_template_id]
                    )
                else:
                    lt_resp = ec2.describe_launch_templates(
                        LaunchTemplateNames=[self.launch_template_name]
                    )

                lts = lt_resp.get("LaunchTemplates", []) or []
                lt = lts[0] if lts else {}
                num = (
                    lt.get("LatestVersionNumber")
                    if version == "$Latest"
                    else lt.get("DefaultVersionNumber")
                )
                if num is not None:
                    version = str(num)

            if self.launch_template_id:
                return {
                    "LaunchTemplateId": self.launch_template_id,
                    "Version": version,
                }
            if self.launch_template_name:
                return {
                    "LaunchTemplateName": self.launch_template_name,
                    "Version": version,
                }
            raise ValueError(
                f"launch_template_id or launch_template_name must be specified for ASG '{self.name}'"
            )

        def _resolve_subnet_ids(self) -> List[str]:
            if self.subnet_ids:
                return self.subnet_ids

            if not self.vpc_id and not self.vpc_name:
                raise ValueError(
                    f"subnet_ids must be specified (non-empty) for ASG '{self.name}' or provide vpc_id/vpc_name for subnet discovery"
                )

            import boto3

            ec2 = boto3.client("ec2", region_name=self.region)

            vpc_id = self.vpc_id
            if not vpc_id:
                vpcs = ec2.describe_vpcs(
                    Filters=[
                        {"Name": "tag:Name", "Values": [self.vpc_name]},
                    ]
                ).get("Vpcs", [])
                if not vpcs:
                    raise Exception(
                        f"Failed to find VPC with tag Name={self.vpc_name} in region {self.region}"
                    )
                if len(vpcs) > 1:
                    raise Exception(
                        f"More than one VPC matched tag Name={self.vpc_name} in region {self.region}"
                    )
                vpc_id = vpcs[0].get("VpcId", "")
                if not vpc_id:
                    raise Exception(
                        f"Failed to resolve VpcId for VPC Name={self.vpc_name}"
                    )

            subnet_filters = [{"Name": "vpc-id", "Values": [vpc_id]}]
            if self.availability_zones:
                subnet_filters.append(
                    {"Name": "availability-zone", "Values": self.availability_zones}
                )

            subnets = ec2.describe_subnets(Filters=subnet_filters).get("Subnets", [])
            subnet_ids = [s.get("SubnetId") for s in subnets if s.get("SubnetId")]
            if not subnet_ids:
                raise Exception(
                    f"Failed to find any subnets in VPC {vpc_id} (AZ filter={self.availability_zones or 'any'})"
                )

            self.ext["resolved_vpc_id"] = vpc_id
            self.ext["resolved_subnet_ids"] = subnet_ids
            return subnet_ids

        def deploy(self):
            """
            Create or update an Auto Scaling Group.

            Notes:
                - This component intentionally does not try to manage every ASG attribute.
                - It focuses on core runner-like ASG needs: subnets, LT, capacity, target groups, tags.
            """
            import boto3
            from botocore.config import Config

            subnet_ids = self._resolve_subnet_ids()

            # Reduce AWS API retries to avoid long "hangs" on transient/opaque InternalFailure.
            # We want the error to surface quickly with the request payload printed below.
            asg_client = boto3.client(
                "autoscaling",
                region_name=self.region,
                config=Config(retries={"max_attempts": 1, "mode": "standard"}),
            )

            vpc_zone_identifier = ",".join(subnet_ids)
            launch_template = self._build_launch_template_spec()

            desired_capacity = (
                self.desired_capacity
                if self.desired_capacity is not None
                else self.min_size
            )

            # Try to fetch existing ASG first
            exists = False
            try:
                self.fetch()
                exists = True
                print(f"Fetched existing configuration for ASG: {self.name}")
            except Exception:
                print(f"ASG {self.name} does not exist yet, will create new")

            if exists:
                print(f"Updating ASG: {self.name}")
                req: Dict[str, Any] = {
                    "AutoScalingGroupName": self.name,
                    "MinSize": self.min_size,
                    "MaxSize": self.max_size,
                    "DesiredCapacity": desired_capacity,
                    "VPCZoneIdentifier": vpc_zone_identifier,
                    "HealthCheckType": self.health_check_type,
                    "HealthCheckGracePeriod": self.health_check_grace_period_sec,
                    "LaunchTemplate": launch_template,
                }
                print(
                    f"ASG '{self.name}': UpdateAutoScalingGroup request: {req}",
                    flush=True,
                )
                asg_client.update_auto_scaling_group(**req)
                print(f"Successfully updated ASG: {self.name}")
            else:
                print(f"Creating new ASG: {self.name}")
                req = {
                    "AutoScalingGroupName": self.name,
                    "MinSize": self.min_size,
                    "MaxSize": self.max_size,
                    "DesiredCapacity": desired_capacity,
                    "VPCZoneIdentifier": vpc_zone_identifier,
                    "HealthCheckType": self.health_check_type,
                    "HealthCheckGracePeriod": self.health_check_grace_period_sec,
                    "LaunchTemplate": launch_template,
                }
                if self.target_group_arns:
                    req["TargetGroupARNs"] = list(self.target_group_arns)

                print(
                    f"ASG '{self.name}': CreateAutoScalingGroup request: {req}",
                    flush=True,
                )
                asg_client.create_auto_scaling_group(**req)
                print(f"Successfully created ASG: {self.name}")

            # Merge mandatory runner identification tags with user-defined tags
            merged_tags = {"praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged_tags["praktika_resource_tag"] = self.praktika_resource_tag
            if self.runner_type:
                merged_tags["github:runner-type"] = self.runner_type
            merged_tags.update(self.tags or {})

            if merged_tags:
                tag_specs = []
                for k, v in merged_tags.items():
                    tag_specs.append(
                        {
                            "ResourceId": self.name,
                            "ResourceType": "auto-scaling-group",
                            "Key": k,
                            "Value": v,
                            "PropagateAtLaunch": True,
                        }
                    )

                asg_client.create_or_update_tags(Tags=tag_specs)
                print(
                    f"Updated {len(merged_tags)} tag(s) (PropagateAtLaunch=True) for ASG: {self.name}"
                )

            return self
