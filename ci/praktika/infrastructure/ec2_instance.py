from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class EC2Instance:

    @dataclass
    class Config:
        # Stable logical name (used for discovery via tags)
        name: str
        region: str = ""

        # Number of similar instances to create (default=1)
        # All instances share the same name and tags
        quantity: int = 1

        # Mandatory runner identification fields
        praktika_resource_tag: str = (
            ""  # Praktika resource tag (e.g., "mac") - tagged as "praktika"
        )
        runner_type: str = (
            ""  # GitHub runner type (e.g., "arm_macos_small") - tagged as "github:runner-type"
        )

        # AMI + instance type
        image_id: str = ""
        instance_type: str = ""

        # Networking
        subnet_id: str = ""
        security_group_ids: List[str] = field(default_factory=list)

        # IAM
        iam_instance_profile_name: str = ""

        # Misc
        key_name: str = ""
        user_data: str = ""
        user_data_file: str = ""  # Path to file containing user_data script

        # Root volume (EBS) settings (optional)
        root_device_name: str = ""  # if empty, resolved from AMI
        root_volume_size: int = 0
        root_volume_type: str = ""  # e.g. gp3
        root_volume_encrypted: bool = False

        # Placement
        tenancy: str = ""  # e.g. "host"

        # TODO: support host_id and host_resource_group_name if auto_placement is not sufficient
        host_id: str = ""
        host_resource_group_name: str = ""

        # Desired behavior
        start_on_deploy: bool = True

        # Tags applied to the instance
        tags: Dict[str, str] = field(default_factory=dict)

        # Extra fetched/derived properties
        ext: Dict[str, Any] = field(default_factory=dict)

        def _merged_tags(self) -> Dict[str, str]:
            merged = {"Name": self.name, "praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged["praktika_resource_tag"] = self.praktika_resource_tag
            if self.runner_type:
                merged["github:runner-type"] = self.runner_type
            # Add user-defined tags
            merged.update(self.tags or {})
            return merged

        def _resolve_host_resource_group_arn(self) -> str:
            if self.ext.get("host_resource_group_arn"):
                return self.ext["host_resource_group_arn"]

            if not self.host_resource_group_name:
                return ""

            import boto3

            rg = boto3.client("resource-groups", region_name=self.region)
            resp = rg.get_group(GroupName=self.host_resource_group_name)
            group = resp.get("Group") or {}
            arn = group.get("GroupArn", "")
            if not arn:
                raise Exception(
                    f"Failed to resolve GroupArn for Resource Group '{self.host_resource_group_name}'"
                )
            self.ext["host_resource_group_arn"] = arn
            return arn

        def _resolve_root_device_name(self) -> str:
            if self.root_device_name:
                return self.root_device_name
            if not self.image_id:
                return ""

            import boto3

            ec2 = boto3.client("ec2", region_name=self.region)
            resp = ec2.describe_images(ImageIds=[self.image_id])
            images = resp.get("Images", []) or []
            root = (images[0] if images else {}).get("RootDeviceName", "")
            if root:
                self.root_device_name = root
            return self.root_device_name

        def _desired_placement(self) -> Dict[str, Any]:
            placement: Dict[str, Any] = {}

            if self.tenancy:
                placement["Tenancy"] = self.tenancy

            if self.host_id:
                placement["Tenancy"] = "host"
                placement["HostId"] = self.host_id
                return placement

            host_rg_arn = self._resolve_host_resource_group_arn()
            if host_rg_arn:
                placement["Tenancy"] = "host"
                placement["HostResourceGroupArn"] = host_rg_arn

            return placement

        def _find_existing_instances(self) -> List[Dict[str, Any]]:
            """Find all existing instances matching the name."""
            import boto3

            ec2 = boto3.client("ec2", region_name=self.region)

            filters = [
                {
                    "Name": "instance-state-name",
                    "Values": ["pending", "running", "stopping", "stopped"],
                },
                {"Name": "tag:Name", "Values": [self.name]},
            ]

            resp = ec2.describe_instances(Filters=filters)
            reservations = resp.get("Reservations", []) or []
            instances: List[Dict[str, Any]] = []
            for r in reservations:
                for inst in r.get("Instances", []) or []:
                    if inst.get("InstanceId"):
                        instances.append(inst)

            instances.sort(key=lambda i: i.get("LaunchTime") or 0, reverse=True)
            return instances

        def _find_existing_instance(self) -> Optional[Dict[str, Any]]:
            """Find existing instance. Raises error if more than one found (unless count > 1)."""
            instances = self._find_existing_instances()

            if not instances:
                return None

            if len(instances) > 1 and self.quantity == 1:
                ids = [i.get("InstanceId") for i in instances if i.get("InstanceId")]
                raise Exception(
                    f"More than one EC2 instance matched Name={self.name}. Delete duplicates to make Name unique. Matched: {ids}"
                )

            return instances[0]

        def fetch(self):
            instances = self._find_existing_instances()
            if not instances:
                raise Exception(f"EC2 Instance '{self.name}' not found in AWS")

            # Store as list if multiple instances, otherwise single value for backwards compatibility
            if len(instances) > 1:
                self.ext["instance_ids"] = [
                    inst.get("InstanceId") for inst in instances
                ]
                self.ext["states"] = [
                    (inst.get("State") or {}).get("Name") for inst in instances
                ]
                self.ext["private_ips"] = [
                    inst.get("PrivateIpAddress") for inst in instances
                ]
                self.ext["public_ips"] = [
                    inst.get("PublicIpAddress") for inst in instances
                ]
                self.ext["launch_times"] = [
                    inst.get("LaunchTime") for inst in instances
                ]
            else:
                inst = instances[0]
                self.ext["instance_id"] = inst.get("InstanceId")
                self.ext["state"] = (inst.get("State") or {}).get("Name")
                self.ext["private_ip"] = inst.get("PrivateIpAddress")
                self.ext["public_ip"] = inst.get("PublicIpAddress")
                self.ext["launch_time"] = inst.get("LaunchTime")
            return self

        def deploy(self):
            import boto3
            import os

            if not self.image_id or not self.instance_type:
                raise ValueError(
                    f"image_id and instance_type must be set for EC2Instance '{self.name}'"
                )

            # Read user_data from file if user_data_file is specified
            if self.user_data_file and not self.user_data:
                if not os.path.isabs(self.user_data_file):
                    # If path is relative, make it absolute from current working directory
                    self.user_data_file = os.path.abspath(self.user_data_file)

                if not os.path.exists(self.user_data_file):
                    raise FileNotFoundError(
                        f"user_data_file '{self.user_data_file}' not found for EC2Instance '{self.name}'"
                    )

                with open(self.user_data_file, "r") as f:
                    self.user_data = f.read()

                print(
                    f"EC2Instance '{self.name}': loaded user_data from file '{self.user_data_file}' ({len(self.user_data)} bytes)"
                )

            existing_instances = self._find_existing_instances()
            ec2 = boto3.client("ec2", region_name=self.region)
            if existing_instances:
                instance_ids = [inst.get("InstanceId") for inst in existing_instances]
                states = [
                    (inst.get("State") or {}).get("Name") for inst in existing_instances
                ]

                # Store as list if multiple instances, otherwise single value for backwards compatibility
                if len(existing_instances) > 1:
                    self.ext["instance_ids"] = instance_ids
                    self.ext["states"] = states
                else:
                    self.ext["instance_id"] = instance_ids[0] if instance_ids else None
                    self.ext["state"] = states[0] if states else None

                # Update tags on all existing instances
                merged_tags = self._merged_tags()
                if instance_ids:
                    ec2.create_tags(
                        Resources=instance_ids,
                        Tags=[{"Key": k, "Value": v} for k, v in merged_tags.items()],
                    )
                    print(
                        f"EC2Instance '{self.name}': ensured {len(merged_tags)} tag(s) on {len(instance_ids)} existing instance(s)"
                    )

                missing = self.quantity - len(existing_instances)
                if missing <= 0:
                    print(
                        f"EC2Instance '{self.name}': found {len(existing_instances)} existing instance(s) - skip create"
                    )

                    # Start stopped instances if needed
                    if self.start_on_deploy:
                        stopped_ids = [
                            inst.get("InstanceId")
                            for inst in existing_instances
                            if (inst.get("State") or {}).get("Name") == "stopped"
                        ]
                        if stopped_ids:
                            print(
                                f"EC2Instance '{self.name}': starting {len(stopped_ids)} stopped instance(s)"
                            )
                            ec2.start_instances(InstanceIds=stopped_ids)

                    return self

                print(
                    f"EC2Instance '{self.name}': found {len(existing_instances)} existing instance(s),"
                    f" need {self.quantity} - creating {missing} more"
                )
            else:
                missing = self.quantity

            req: Dict[str, Any] = {
                "ImageId": self.image_id,
                "InstanceType": self.instance_type,
                "MinCount": missing,
                "MaxCount": missing,
            }

            if self.subnet_id:
                req["SubnetId"] = self.subnet_id

            if self.security_group_ids:
                req["SecurityGroupIds"] = list(self.security_group_ids)

            if self.iam_instance_profile_name:
                req["IamInstanceProfile"] = {"Name": self.iam_instance_profile_name}

            if self.key_name:
                req["KeyName"] = self.key_name

            if self.user_data:
                req["UserData"] = self.user_data

            if (
                self.root_volume_size
                or self.root_volume_type
                or self.root_volume_encrypted
            ):
                device_name = self._resolve_root_device_name()
                if not device_name:
                    raise ValueError(
                        f"Failed to resolve root_device_name for EC2Instance '{self.name}' (image_id={self.image_id})"
                    )

                ebs: Dict[str, Any] = {}
                if self.root_volume_size:
                    ebs["VolumeSize"] = int(self.root_volume_size)
                if self.root_volume_type:
                    ebs["VolumeType"] = self.root_volume_type
                if self.root_volume_encrypted:
                    ebs["Encrypted"] = True

                req["BlockDeviceMappings"] = [
                    {
                        "DeviceName": device_name,
                        "Ebs": ebs,
                    }
                ]

            placement = self._desired_placement()
            if placement:
                req["Placement"] = placement

            merged_tags = self._merged_tags()
            req["TagSpecifications"] = [
                {
                    "ResourceType": "instance",
                    "Tags": [{"Key": k, "Value": v} for k, v in merged_tags.items()],
                }
            ]

            resp = ec2.run_instances(**req)
            instances = resp.get("Instances", []) or []

            if not instances:
                raise Exception(
                    f"EC2Instance '{self.name}': failed to get instances from RunInstances response"
                )

            instance_ids = [
                inst.get("InstanceId") for inst in instances if inst.get("InstanceId")
            ]
            states = [(inst.get("State") or {}).get("Name") for inst in instances]

            # Store as list if multiple instances, otherwise single value for backwards compatibility
            if len(instances) > 1:
                self.ext["instance_ids"] = instance_ids
                self.ext["states"] = states
            else:
                self.ext["instance_id"] = instance_ids[0] if instance_ids else None
                self.ext["state"] = states[0] if states else None

            if not self.start_on_deploy and instance_ids:
                print(
                    f"EC2Instance '{self.name}': stopping {len(instance_ids)} instance(s) (start_on_deploy=False)"
                )
                ec2.stop_instances(InstanceIds=instance_ids)

            print(
                f"EC2Instance '{self.name}': launched {len(instance_ids)} instance(s): {instance_ids}"
            )
            return self

        def shutdown(self, force: bool = True):
            """
            Terminate all EC2 instances matching this configuration.

            Args:
                force: If True, forcefully terminate without stopping first (default: True).
            """
            import boto3

            existing_instances = self._find_existing_instances()
            if not existing_instances:
                print(
                    f"EC2Instance '{self.name}': no instances found - nothing to shutdown"
                )
                return self

            instance_ids = [
                inst.get("InstanceId")
                for inst in existing_instances
                if inst.get("InstanceId")
            ]
            if not instance_ids:
                print(
                    f"EC2Instance '{self.name}': no valid instance IDs - skip shutdown"
                )
                return self

            ec2 = boto3.client("ec2", region_name=self.region)

            print(
                f"EC2Instance '{self.name}': found {len(instance_ids)} instance(s) to shutdown: {instance_ids}"
            )

            # If force is True, terminate directly without stopping
            if force:
                print(
                    f"EC2Instance '{self.name}': forcefully terminating {len(instance_ids)} instance(s)"
                )
                ec2.terminate_instances(InstanceIds=instance_ids)
                print(
                    f"EC2Instance '{self.name}': {len(instance_ids)} instance(s) terminated"
                )
            else:
                # Stop running instances first
                running_ids = [
                    inst.get("InstanceId")
                    for inst in existing_instances
                    if (inst.get("State") or {}).get("Name") in ["pending", "running"]
                ]
                if running_ids:
                    print(
                        f"EC2Instance '{self.name}': stopping {len(running_ids)} running instance(s)"
                    )
                    ec2.stop_instances(InstanceIds=running_ids)
                    print(
                        f"EC2Instance '{self.name}': {len(running_ids)} instance(s) stopped"
                    )

                # Then terminate all
                print(
                    f"EC2Instance '{self.name}': terminating {len(instance_ids)} instance(s)"
                )
                ec2.terminate_instances(InstanceIds=instance_ids)
                print(
                    f"EC2Instance '{self.name}': {len(instance_ids)} instance(s) terminated"
                )

            return self
