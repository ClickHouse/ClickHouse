from ._utils import aws_client
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

            asg_client = aws_client("autoscaling", self.region, self.name)

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
            version = self.launch_template_version or "$Default"
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

            ec2 = aws_client("ec2", self.region, self.name)

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

        def _desired_tags(self) -> Dict[str, str]:
            merged_tags = {"praktika_rn": self.name}
            if self.praktika_resource_tag:
                merged_tags["praktika_resource_tag"] = self.praktika_resource_tag
            if self.runner_type:
                merged_tags["github:runner-type"] = self.runner_type
            merged_tags.update(self.tags or {})
            return merged_tags

        def _launch_template_matches(
            self,
            current: Dict[str, Any],
            desired: Dict[str, Any],
        ) -> bool:
            current_id = current.get("id") or current.get("LaunchTemplateId") or ""
            current_name = current.get("name") or current.get("LaunchTemplateName") or ""
            current_version = str(
                current.get("version") or current.get("Version") or ""
            )
            desired_id = desired.get("LaunchTemplateId", "")
            desired_name = desired.get("LaunchTemplateName", "")
            desired_version = str(desired.get("Version", ""))

            if desired_id and current_id and desired_id != current_id:
                return False
            if desired_name and current_name and desired_name != current_name:
                return False
            if desired_id and not current_id and current_name:
                return False
            if desired_name and not current_name and current_id:
                return False

            if desired_version in {"$Default", "$Latest"}:
                return True
            return current_version == desired_version

        def _is_up_to_date(
            self,
            *,
            vpc_zone_identifier: str,
            launch_template: Dict[str, str],
            desired_capacity: int,
            desired_tags: Dict[str, str],
        ) -> bool:
            current_launch_template = self.ext.get("launch_template", {})
            current_target_groups = sorted(self.ext.get("target_group_arns", []) or [])
            desired_target_groups = sorted(self.target_group_arns or [])
            current_subnets = sorted(
                [s for s in (self.ext.get("vpc_zone_identifier", "") or "").split(",") if s]
            )
            desired_subnets = sorted(
                [s for s in (vpc_zone_identifier or "").split(",") if s]
            )

            return (
                self.ext.get("min_size") == self.min_size
                and self.ext.get("max_size") == self.max_size
                and self.ext.get("desired_capacity") == desired_capacity
                and self.ext.get("health_check_type") == self.health_check_type
                and self.ext.get("health_check_grace_period")
                == self.health_check_grace_period_sec
                and current_subnets == desired_subnets
                and self._launch_template_matches(current_launch_template, launch_template)
                and current_target_groups == desired_target_groups
                and (self.ext.get("tags") or {}) == desired_tags
            )

        def deploy(self):
            """
            Create or update an Auto Scaling Group.

            Notes:
                - This component intentionally does not try to manage every ASG attribute.
                - It focuses on core runner-like ASG needs: subnets, LT, capacity, target groups, tags.
            """
            import boto3
            from botocore.config import Config

            self.ext.pop("deferred_missing_launch_template", None)
            self.ext.pop("deployment_warning", None)

            subnet_ids = self._resolve_subnet_ids()

            # Reduce AWS API retries to avoid long "hangs" on transient/opaque InternalFailure.
            # We want the error to surface quickly with the request payload printed below.
            asg_client = aws_client(
                "autoscaling", self.region, self.name,
                config=Config(retries={"max_attempts": 1, "mode": "standard"}),
            )

            vpc_zone_identifier = ",".join(subnet_ids)
            launch_template = self._build_launch_template_spec()

            desired_capacity = (
                self.desired_capacity
                if self.desired_capacity is not None
                else self.min_size
            )
            desired_tags = self._desired_tags()

            def _is_missing_launch_template_error(exc: Exception) -> bool:
                message = str(exc).lower()
                return (
                    "launch template" in message
                    and "does not exist" in message
                )

            def _defer_missing_launch_template():
                warning = (
                    f"Launch Template is not available yet for ASG '{self.name}'; "
                    "skipping until the launch template exists"
                )
                self.ext["deferred_missing_launch_template"] = True
                self.ext["deployment_warning"] = warning
                print(f"WARNING: {warning}")
                return self

            # Try to fetch existing ASG first
            exists = False
            try:
                self.fetch()
                exists = True
                print(f"Fetched existing configuration for ASG: {self.name}")
            except Exception:
                print(f"ASG {self.name} does not exist yet, will create new")

            if exists:
                if self._is_up_to_date(
                    vpc_zone_identifier=vpc_zone_identifier,
                    launch_template=launch_template,
                    desired_capacity=desired_capacity,
                    desired_tags=desired_tags,
                ):
                    print(f"ASG '{self.name}' is already up to date, skipping")
                    return self
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
                try:
                    asg_client.update_auto_scaling_group(**req)
                except Exception as e:
                    if _is_missing_launch_template_error(e):
                        return _defer_missing_launch_template()
                    raise
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
                try:
                    asg_client.create_auto_scaling_group(**req)
                except Exception as e:
                    if _is_missing_launch_template_error(e):
                        return _defer_missing_launch_template()
                    raise
                print(f"Successfully created ASG: {self.name}")

            if desired_tags:
                tag_specs = []
                for k, v in desired_tags.items():
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
                    f"Updated {len(desired_tags)} tag(s) (PropagateAtLaunch=True) for ASG: {self.name}"
                )

            return self

        def restart(self):
            """Start an instance refresh to replace all instances with the current LT version."""
            client = aws_client("autoscaling", self.region, self.name)
            try:
                resp = client.start_instance_refresh(
                    AutoScalingGroupName=self.name,
                    Preferences={
                        "MinHealthyPercentage": 0,
                        "InstanceWarmup": 60,
                    },
                )
                refresh_id = resp.get("InstanceRefreshId", "")
                print(f"Instance refresh started for ASG '{self.name}' (id={refresh_id})")
            except client.exceptions.ClientError as e:
                if "InstanceRefreshInProgress" in str(e):
                    print(f"Instance refresh already in progress for ASG '{self.name}', skipping")
                else:
                    raise
            return self

        def delete(self):
            import boto3
            client = aws_client("autoscaling", self.region, self.name)
            try:
                client.delete_auto_scaling_group(
                    AutoScalingGroupName=self.name, ForceDelete=True
                )
                print(f"Deleted Auto Scaling Group '{self.name}'")
            except client.exceptions.ClientError as e:
                if "not found" in str(e).lower():
                    print(f"Auto Scaling Group '{self.name}' does not exist, skipping")
                else:
                    raise
