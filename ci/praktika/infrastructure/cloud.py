from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional

from ..settings import _Settings
from .autoscaling_group import AutoScalingGroup
from .dedicated_host import DedicatedHost
from .ec2_instance import EC2Instance
from .iam_instance_profile import IAMInstanceProfile
from .image_builder import ImageBuilder
from .lambda_function import lambda_app_config, lambda_worker_config
from .launch_template import LaunchTemplate

if TYPE_CHECKING:
    from .autoscaling_group import AutoScalingGroup
    from .dedicated_host import DedicatedHost
    from .ec2_instance import EC2Instance
    from .iam_instance_profile import IAMInstanceProfile
    from .image_builder import ImageBuilder
    from .lambda_function import Lambda
    from .launch_template import LaunchTemplate


class CloudInfrastructure:
    SLACK_APP_LAMBDAS = [lambda_app_config, lambda_worker_config]

    @dataclass
    class Config:
        name: str
        lambda_functions: List["Lambda.Config"] = field(default_factory=list)
        iam_instance_profiles: List["IAMInstanceProfile.Config"] = field(
            default_factory=list
        )
        dedicated_hosts: List["DedicatedHost.Config"] = field(default_factory=list)
        ec2_instances: List["EC2Instance.Config"] = field(default_factory=list)
        image_builders: List["ImageBuilder.Config"] = field(default_factory=list)
        launch_templates: List["LaunchTemplate.Config"] = field(default_factory=list)
        autoscaling_groups: List["AutoScalingGroup.Config"] = field(
            default_factory=list
        )
        _settings: Optional[_Settings] = None

        def deploy(
            self,
            all=False,
            only: Optional[List[str]] = None,
        ):
            """
            Deploy Lambda functions.

            Args:
                all: If False, only deploy code (skip settings validation and IAM policies).
                     If True, deploy everything (validate settings, deploy code, attach IAM policies).
                only: If set, deploy only selected component types by name.
            """
            only_set = {
                s.strip().lower()
                for s in (only or [])
                if isinstance(s, str) and s.strip()
            }

            def _wants(type_name: str, *aliases: str) -> bool:
                if not only_set:
                    return True
                keys = {type_name.lower(), *{a.lower() for a in aliases if a}}
                return bool(keys & only_set)

            if (
                not self.lambda_functions
                and not self.iam_instance_profiles
                and not self.dedicated_hosts
                and not self.ec2_instances
                and not self.image_builders
                and not self.launch_templates
                and not self.autoscaling_groups
            ):
                print("No infrastructure components to deploy")
                return

            # Full deployment mode: validate settings and configure environments
            if all:
                if not self._settings:
                    raise ValueError(
                        "Settings not configured. Please set _settings before deploying."
                    )

                required_settings = {
                    "EVENT_FEED_S3_PATH": self._settings.EVENT_FEED_S3_PATH,
                    "AWS_REGION": self._settings.AWS_REGION,
                }

                missing_settings = [
                    name for name, value in required_settings.items() if not value
                ]
                if missing_settings:
                    raise ValueError(
                        f"Missing required settings for Lambda deployment: {', '.join(missing_settings)}"
                    )

            # Deploy all Dedicated Hosts
            if _wants("DedicatedHost", "DedicatedHosts"):
                for host_config in self.dedicated_hosts:
                    if self._settings and self._settings.AWS_REGION:
                        host_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Dedicated Hosts: {host_config.name}")
                    print("=" * 60)
                    host_config.deploy()

            # Deploy IAM Instance Profiles (before EC2 instances that may reference them)
            if _wants("IAMInstanceProfile", "IAMInstanceProfiles"):
                for ip_config in self.iam_instance_profiles:
                    if self._settings and self._settings.AWS_REGION:
                        ip_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying IAM Instance Profile: {ip_config.name}")
                    print("=" * 60)
                    ip_config.deploy()

            # Deploy EC2 Instances
            if _wants("EC2Instance", "EC2Instances", "Instance", "Instances"):
                for instance_config in self.ec2_instances:
                    if self._settings and self._settings.AWS_REGION:
                        instance_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying EC2 Instance: {instance_config.name}")
                    print("=" * 60)
                    instance_config.deploy()

            # Deploy Image Builder pipelines
            if _wants("ImageBuilder", "ImageBuilders"):
                for ib_config in self.image_builders:
                    if self._settings and self._settings.AWS_REGION:
                        ib_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Image Builder: {ib_config.name}")
                    print("=" * 60)
                    ib_config.deploy()

            # Deploy all Launch Templates
            if _wants("LaunchTemplate", "LaunchTemplates"):
                for lt_config in self.launch_templates:
                    if self._settings and self._settings.AWS_REGION:
                        lt_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Launch Template: {lt_config.name}")
                    print("=" * 60)
                    lt_config.deploy()

            # Deploy all ASGs
            if _wants("AutoScalingGroup", "AutoScalingGroups", "ASG", "ASGs"):
                for asg_config in self.autoscaling_groups:
                    if self._settings and self._settings.AWS_REGION:
                        asg_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Auto Scaling Group: {asg_config.name}")
                    print("=" * 60)
                    asg_config.deploy()

            # Deploy all Lambdas (code only or with configuration)
            if _wants("Lambda", "Lambdas", "LambdaFunction", "LambdaFunctions"):
                for lambda_config in self.lambda_functions:
                    # Always set region if available (needed even for code-only deploys)
                    if self._settings and self._settings.AWS_REGION:
                        lambda_config.region = self._settings.AWS_REGION

                    # Only set environment variables in full deployment mode
                    if all and self._settings:
                        if self._settings.EVENT_FEED_S3_PATH:
                            # Inject project-specific settings into Lambda environment
                            # EVENT_FEED_S3_PATH is required by Slack Lambdas for event feed storage
                            lambda_config.environments["EVENT_FEED_S3_PATH"] = (
                                self._settings.EVENT_FEED_S3_PATH
                            )

                    print("\n" + "=" * 60)
                    print(f"Deploying Lambda: {lambda_config.name}")
                    print("=" * 60)
                    lambda_config.deploy()

            # Only attach IAM policies in full deployment mode
            if all and _wants("Lambda", "Lambdas", "LambdaFunction", "LambdaFunctions"):
                print("\n" + "=" * 60)
                print("Attaching IAM policies...")
                print("=" * 60)

                for lambda_config in self.lambda_functions:
                    role_arn = lambda_config.ext.get("role_arn")
                    if not role_arn:
                        print(
                            f"Warning: No role_arn found for {lambda_config.name}, skipping policy attachment"
                        )
                        continue

                    # Attach policies based on Lambda name patterns
                    if "worker" in lambda_config.name:
                        # Worker Lambda needs S3 read/write and CloudWatch access
                        lambda_config._attach_s3_readwrite_policy(role_arn)
                        lambda_config._attach_cloudwatch_logs_policy(role_arn)
                    elif "app" in lambda_config.name:
                        # App Lambda needs to invoke worker
                        worker_name = next(
                            (
                                lc.name
                                for lc in self.lambda_functions
                                if "worker" in lc.name
                            ),
                            None,
                        )
                        if worker_name:
                            lambda_config._attach_worker_invoke_policy(
                                role_arn, worker_name
                            )

                print("\n" + "=" * 60)
                print("Lambda deployment completed!")
                print("=" * 60)

        def shutdown(
            self,
            force: bool = True,
            only: Optional[List[str]] = None,
        ):
            """
            Terminate running EC2 instances and release Dedicated Hosts.

            Args:
                force: If True, forcefully terminate instances without stopping first (default: True).
                only: If set, shutdown only selected component types by name (e.g. EC2Instance, DedicatedHost).
            """
            only_set = {
                s.strip().lower()
                for s in (only or [])
                if isinstance(s, str) and s.strip()
            }

            def _wants(type_name: str, *aliases: str) -> bool:
                if not only_set:
                    return True
                keys = {type_name.lower(), *{a.lower() for a in aliases if a}}
                return bool(keys & only_set)

            has_work = bool(self.ec2_instances or self.dedicated_hosts)
            if not has_work:
                print("No resources configured to shutdown")
                return

            # Shutdown Dedicated Hosts
            if self.dedicated_hosts and _wants("DedicatedHost", "DedicatedHosts"):
                for host_config in self.dedicated_hosts:
                    if self._settings and self._settings.AWS_REGION:
                        host_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Shutting down Dedicated Host pool: {host_config.name}")
                    print("=" * 60)
                    host_config.shutdown(force=force)

            # Shutdown EC2 Instances
            if self.ec2_instances and _wants(
                "EC2Instance", "EC2Instances", "Instance", "Instances"
            ):
                for instance_config in self.ec2_instances:
                    if self._settings and self._settings.AWS_REGION:
                        instance_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Shutting down EC2 Instance: {instance_config.name}")
                    print("=" * 60)
                    instance_config.shutdown(force=force)

            print("\n" + "=" * 60)
            print("Shutdown completed!")
            print("=" * 60)
