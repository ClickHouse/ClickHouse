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
        # GitHub runner labels (e.g., ["arm_macos_small", "macos"]) - tagged as "github:runner-type"
        # (comma-separated). Tag key is kept for compatibility with the legacy runner-init.py,
        # which inlines this value into the runner's `--labels` list.
        runner_labels: List[str] = field(default_factory=list)

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

        # If True, deploy() will stop existing instances whose live UserData differs
        # from `user_data`, call ModifyInstanceAttribute to install the new UserData,
        # and then start them again (subject to `start_on_deploy`). Useful for
        # instance types like mac1/mac2 where terminate + recreate is expensive due
        # to dedicated-host cooldown.
        update_user_data_on_change: bool = False

        # Tags applied to the instance
        tags: Dict[str, str] = field(default_factory=dict)

        # Extra fetched/derived properties
        ext: Dict[str, Any] = field(default_factory=dict)

        def _merged_tags(self) -> Dict[str, str]:
            merged = {"Name": self.name, "praktika_rn": self.name}
            # Add resource tag if specified
            if self.praktika_resource_tag:
                merged["praktika_resource_tag"] = self.praktika_resource_tag
            if self.runner_labels:
                merged["github:runner-type"] = ",".join(self.runner_labels)
            # Add user-defined tags
            merged.update(self.tags or {})
            return merged

        def _sync_tags(self, ec2, instance_ids: List[str]) -> None:
            """Upsert desired tags and remove any other tags found on the instances.

            AWS-managed tags (`aws:*`) are skipped — they cannot be deleted and
            are not under our control. Any other tag not present in the desired
            set is treated as stale and removed, so the config is the source of
            truth.
            """
            if not instance_ids:
                return

            desired = self._merged_tags()
            ec2.create_tags(
                Resources=instance_ids,
                Tags=[{"Key": k, "Value": v} for k, v in desired.items()],
            )
            print(
                f"EC2Instance '{self.name}': ensured {len(desired)} tag(s) on {len(instance_ids)} instance(s)"
            )

            resp = ec2.describe_tags(
                Filters=[{"Name": "resource-id", "Values": instance_ids}]
            )
            desired_keys = set(desired.keys())
            stale: Dict[str, List[str]] = {}
            for t in resp.get("Tags", []) or []:
                key = t.get("Key", "")
                if not key or key.startswith("aws:") or key in desired_keys:
                    continue
                rid = t.get("ResourceId")
                if rid:
                    stale.setdefault(key, []).append(rid)
            for key, ids in stale.items():
                ec2.delete_tags(Resources=ids, Tags=[{"Key": key}])
                print(
                    f"EC2Instance '{self.name}': removed stale tag '{key}' from {len(ids)} instance(s)"
                )

        def _sync_iam_instance_profile(
            self, ec2, instances: List[Dict[str, Any]]
        ) -> None:
            """Reconcile IAM instance profile association on existing instances.

            Associates/replaces the profile to match `iam_instance_profile_name`,
            or disassociates if the config is empty. No-op if already matching.

            Only acts on instances in `running`/`pending` state — AWS rejects
            `ReplaceIamInstanceProfileAssociation` with IncorrectState on stopped
            instances.
            """
            instance_ids = []
            for inst in instances:
                iid = inst.get("InstanceId")
                state = (inst.get("State") or {}).get("Name", "")
                if not iid:
                    continue
                if state in ("running", "pending"):
                    instance_ids.append(iid)
                else:
                    print(
                        f"EC2Instance '{self.name}': skip IAM profile sync on {iid} (state={state or 'unknown'})"
                    )
            if not instance_ids:
                return

            desired = self.iam_instance_profile_name
            # Filter to currently-active associations only. Transitional states
            # (`associating`, `disassociating`) can be returned by the API but
            # cannot be replaced/disassociated and would yield IncorrectState.
            resp = ec2.describe_iam_instance_profile_associations(
                Filters=[
                    {"Name": "instance-id", "Values": instance_ids},
                    {"Name": "state", "Values": ["associated"]},
                ]
            )
            assocs: Dict[str, Dict[str, Any]] = {}
            for a in resp.get("IamInstanceProfileAssociations", []) or []:
                iid = a.get("InstanceId")
                if iid:
                    assocs[iid] = a

            for iid in instance_ids:
                a = assocs.get(iid) or {}
                arn = (a.get("IamInstanceProfile") or {}).get("Arn", "")
                current = arn.rsplit("/", 1)[-1] if arn else ""
                aid = a.get("AssociationId", "")
                active = bool(aid)

                if desired:
                    if current == desired:
                        continue
                    if active:
                        ec2.replace_iam_instance_profile_association(
                            AssociationId=aid,
                            IamInstanceProfile={"Name": desired},
                        )
                        print(
                            f"EC2Instance '{self.name}': replaced IAM profile on {iid} ({current or 'none'} -> {desired})"
                        )
                    else:
                        ec2.associate_iam_instance_profile(
                            InstanceId=iid,
                            IamInstanceProfile={"Name": desired},
                        )
                        print(
                            f"EC2Instance '{self.name}': associated IAM profile '{desired}' to {iid}"
                        )
                elif active:
                    ec2.disassociate_iam_instance_profile(AssociationId=aid)
                    print(
                        f"EC2Instance '{self.name}': disassociated IAM profile '{current}' from {iid}"
                    )

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

        def _reconcile_user_data(self, ec2, existing_instances, force=False) -> List[str]:
            """If `update_user_data_on_change` is set, compare live UserData on each
            existing instance with `self.user_data`. For mismatches, stop the
            instance (waiting until it is fully stopped) and call
            ModifyInstanceAttribute to install the new UserData. Returns the list
            of instance IDs whose UserData was updated; the caller is responsible
            for starting them back up if needed.

            `force` ignores the `update_user_data_on_change` opt-in (used by the
            `--instance` debug path, where the explicit instance id is itself the
            opt-in); the content comparison still applies, so an instance whose
            live UserData already matches is left running.
            """
            import base64

            if not self.user_data:
                return []
            if not force and not self.update_user_data_on_change:
                return []

            to_update: List[str] = []
            for inst in existing_instances:
                instance_id = inst.get("InstanceId")
                if not instance_id:
                    continue
                resp = ec2.describe_instance_attribute(
                    InstanceId=instance_id, Attribute="userData"
                )
                encoded = (resp.get("UserData") or {}).get("Value") or ""
                if isinstance(encoded, bytes):
                    encoded = encoded.decode("ascii")
                current = (
                    base64.b64decode(encoded).decode("utf-8") if encoded else ""
                )
                if current != self.user_data:
                    to_update.append(instance_id)

            if not to_update:
                return []

            print(
                f"EC2Instance '{self.name}': user_data changed for "
                f"{len(to_update)} instance(s): {to_update}"
            )

            running_ids = [
                inst.get("InstanceId")
                for inst in existing_instances
                if inst.get("InstanceId") in to_update
                and (inst.get("State") or {}).get("Name") in ["pending", "running"]
            ]
            if running_ids:
                print(
                    f"EC2Instance '{self.name}': stopping {len(running_ids)} instance(s) to update user_data"
                )
                ec2.stop_instances(InstanceIds=running_ids)
                ec2.get_waiter("instance_stopped").wait(InstanceIds=running_ids)

            encoded_value = base64.b64encode(self.user_data.encode("utf-8"))
            for instance_id in to_update:
                ec2.modify_instance_attribute(
                    InstanceId=instance_id,
                    UserData={"Value": encoded_value},
                )
            print(
                f"EC2Instance '{self.name}': updated user_data on {len(to_update)} instance(s)"
            )

            return to_update

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

        def deploy(self, only_instance_id: Optional[str] = None):
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

            # Debug path: update user_data on a single, explicitly named instance.
            # Skip create / quantity / tag / IAM reconciliation entirely so the
            # rest of the fleet is left untouched.
            if only_instance_id:
                target = [
                    inst
                    for inst in existing_instances
                    if inst.get("InstanceId") == only_instance_id
                ]
                if not target:
                    print(
                        f"EC2Instance '{self.name}': instance {only_instance_id} is "
                        f"not one of its instances - skip"
                    )
                    return self

                print(
                    f"EC2Instance '{self.name}': --instance {only_instance_id} - "
                    f"updating user_data on this instance only"
                )
                updated_ids = self._reconcile_user_data(ec2, target, force=True)
                if not updated_ids:
                    print(
                        f"EC2Instance '{self.name}': user_data on {only_instance_id} "
                        f"already up to date - nothing to do"
                    )
                elif self.start_on_deploy:
                    # _reconcile_user_data left the instance stopped; start it again.
                    print(
                        f"EC2Instance '{self.name}': starting {only_instance_id} after "
                        f"user_data update"
                    )
                    ec2.start_instances(InstanceIds=updated_ids)
                return self

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

                # Reconcile tags and IAM instance profile on existing instances.
                self._sync_tags(ec2, instance_ids)
                self._sync_iam_instance_profile(ec2, existing_instances)

                missing = self.quantity - len(existing_instances)
                if missing <= 0:
                    print(
                        f"EC2Instance '{self.name}': found {len(existing_instances)} existing instance(s) - skip create"
                    )

                    updated_ids = self._reconcile_user_data(ec2, existing_instances)
                    # The reconciliation left these instances stopped; reflect that
                    # in the local list so the start-stopped block below picks them
                    # up (subject to start_on_deploy).
                    for inst in existing_instances:
                        if inst.get("InstanceId") in updated_ids:
                            inst["State"] = {"Name": "stopped"}

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
                            from botocore.exceptions import ClientError

                            try:
                                ec2.start_instances(InstanceIds=stopped_ids)
                            except ClientError as e:
                                error_code = e.response.get("Error", {}).get("Code", "")
                                if error_code == "InsufficientHostCapacity":
                                    # No dedicated host available — external capacity
                                    # constraint, nothing to fix programmatically.
                                    print(
                                        f"EC2Instance '{self.name}': WARNING no host capacity to start {stopped_ids}: {e}"
                                    )
                                else:
                                    raise
                            else:
                                # Re-reconcile IAM profile on the newly-started instances:
                                # _sync_iam_instance_profile skips stopped instances, so
                                # any profile change is only applied after start.
                                started = ec2.describe_instances(
                                    InstanceIds=stopped_ids
                                )
                                started_instances = [
                                    inst
                                    for r in started.get("Reservations", [])
                                    for inst in r.get("Instances", [])
                                ]
                                self._sync_iam_instance_profile(ec2, started_instances)

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
