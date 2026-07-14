import copy
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..settings import _Settings
from ..version import current_praktika_version, version_key
from ._utils import aws_client
from .autoscaling_group import AutoScalingGroup
from .dedicated_host import DedicatedHost
from .ec2_instance import EC2Instance
from .iam_instance_profile import IAMInstanceProfile
from .iam_role import IAMRole
from .image_builder import ImageBuilder
from .lambda_function import lambda_app_config, lambda_worker_config
from .launch_template import LaunchTemplate
from .sqs_queue import SQSQueue

if TYPE_CHECKING:
    from .autoscaling_group import AutoScalingGroup
    from .report_page import ReportPage
    from .storage import Storage
    from .vpc import VPC
    from .dedicated_host import DedicatedHost
    from .ec2_instance import EC2Instance
    from .iam_instance_profile import IAMInstanceProfile
    from .iam_role import IAMRole
    from .image_builder import ImageBuilder
    from .lambda_function import Lambda
    from .launch_template import LaunchTemplate
    from .native.cidb_cluster import CIDBCluster
    from .native.orchestrator_pool import OrchestratorPool
    from .native.github_token_minter import GitHubTokenMinter
    from .native.pool_autoscaler import PoolAutoscaler
    from .native.runner_pool import RunnerPool
    from .secret_parameter import SecretParameter
    from .sqs_queue import SQSQueue


class CloudInfrastructure:
    SLACK_APP_LAMBDAS = [lambda_app_config, lambda_worker_config]

    @dataclass
    class Config:
        name: str
        min_praktika_version: str = "0.0.0"
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
        sqs_queues: List["SQSQueue.Config"] = field(default_factory=list)
        iam_roles: List["IAMRole.Config"] = field(default_factory=list)
        secret_parameters: List["SecretParameter.Config"] = field(default_factory=list)
        storages: List["Storage.Config"] = field(default_factory=list)
        report_pages: List["ReportPage.Config"] = field(default_factory=list)
        vpcs: List["VPC.Config"] = field(default_factory=list)
        runner_pools: List["RunnerPool"] = field(default_factory=list)
        github_token_minters: List["GitHubTokenMinter"] = field(default_factory=list)
        pool_autoscalers: List["PoolAutoscaler"] = field(default_factory=list)
        pool_autoscaler_interval_seconds: int = 60
        orchestrator_pool: Optional["OrchestratorPool"] = None
        orchestrator_pools: List["OrchestratorPool"] = field(default_factory=list)
        cidb_cluster: Optional["CIDBCluster"] = None
        ext: Dict[str, Any] = field(default_factory=dict)
        _settings: Optional[_Settings] = None
        _pre_namespace_names: Dict[str, List[str]] = field(
            default_factory=dict, init=False, repr=False
        )

        def _clone_owned_configs(self):
            # Cloud configs are often composed from module-level shared objects
            # in ci/infrastructure/projects.py. Namespace application mutates names deeply,
            # so we must detach from those shared objects first; otherwise one
            # project would rewrite another project's config in place.
            cloned = copy.deepcopy(
                {
                    "lambda_functions": self.lambda_functions,
                    "iam_instance_profiles": self.iam_instance_profiles,
                    "dedicated_hosts": self.dedicated_hosts,
                    "ec2_instances": self.ec2_instances,
                    "image_builders": self.image_builders,
                    "launch_templates": self.launch_templates,
                    "autoscaling_groups": self.autoscaling_groups,
                    "sqs_queues": self.sqs_queues,
                    "iam_roles": self.iam_roles,
                    "secret_parameters": self.secret_parameters,
                    "storages": self.storages,
                    "report_pages": self.report_pages,
                    "vpcs": self.vpcs,
                    "runner_pools": self.runner_pools,
                    "github_token_minters": self.github_token_minters,
                    "pool_autoscalers": self.pool_autoscalers,
                    "orchestrator_pool": self.orchestrator_pool,
                    "orchestrator_pools": self.orchestrator_pools,
                    "cidb_cluster": self.cidb_cluster,
                }
            )
            for key, value in cloned.items():
                setattr(self, key, value)

        def _project_prefix(self) -> str:
            # Project names are user-facing labels. Resource names need a
            # stable AWS-safe slug so all generated names line up across
            # queues, ASGs, IAM roles, launch templates, etc.
            #
            # Use "_" (not "-") as the intra-slug separator. The slug becomes
            # the "{slug}-" resource-name prefix, so a "-" inside it would let
            # one project's scoped IAM wildcard (e.g. "clickhouse-*") also match
            # another project's resources (e.g. "clickhouse-private-*").
            prefix = re.sub(r"[^a-z0-9]+", "_", (self.name or "").strip().lower())
            prefix = re.sub(r"_{2,}", "_", prefix).strip("_")
            if not prefix:
                raise ValueError("CloudInfrastructure.Config.name must normalize to a non-empty project prefix")
            return prefix

        def _prefixed(self, value: str) -> str:
            # Idempotent helper: callers can safely run namespace application
            # multiple times without double-prefixing names.
            if not value:
                return value
            prefix = f"{self._project_prefix()}-"
            if value.startswith(prefix):
                return value
            return f"{prefix}{value}"

        def _prefixed_secret_name(self, value: str) -> str:
            # SSM/Secrets-style names may be bare names or "/path"-like names.
            # Keep the leading slash semantics intact while still applying the
            # project namespace to the actual secret name.
            if not value:
                return value
            if value.startswith("/"):
                return "/" + self._prefixed(value.lstrip("/"))
            return self._prefixed(value)

        def _replace_recursive(self, value, replacements):
            # Some configs embed resource names inside user-data scripts,
            # inline IAM policy ARNs, lambda env JSON, etc. After renaming the
            # typed fields above, sweep those nested blobs so references stay
            # consistent with the generated AWS names.
            if isinstance(value, str):
                result = value
                for old, new in sorted(
                    replacements.items(), key=lambda item: len(item[0]), reverse=True
                ):
                    if not old:
                        continue
                    # Only replace the name when it sits at a token boundary —
                    # not preceded or followed by [a-zA-Z0-9-].  This prevents
                    # "artifacts-eu-north-1" from matching inside
                    # "praktika-artifacts-eu-north-1" and corrupting URLs or
                    # other cross-project references embedded in user_data.
                    pattern = rf"(?<![a-zA-Z0-9-]){re.escape(old)}(?![a-zA-Z0-9-])"
                    result = re.sub(pattern, new, result)
                return result
            if isinstance(value, list):
                return [self._replace_recursive(item, replacements) for item in value]
            if isinstance(value, dict):
                return {
                    self._replace_recursive(key, replacements): self._replace_recursive(val, replacements)
                    for key, val in value.items()
                }
            return value

        def _record_rename(self, replacements, old: str, new: str):
            if old and new and old != new:
                replacements[old] = new

        def _capture_pre_namespace_names(self):
            self._pre_namespace_names = {}
            for attr in (
                "vpcs",
                "storages",
                "report_pages",
                "image_builders",
                "launch_templates",
                "autoscaling_groups",
                "sqs_queues",
                "iam_roles",
                "iam_instance_profiles",
                "secret_parameters",
                "lambda_functions",
                "dedicated_hosts",
                "ec2_instances",
                "runner_pools",
                "orchestrator_pools",
                "github_token_minters",
                "pool_autoscalers",
            ):
                names = [
                    item.name
                    for item in getattr(self, attr, []) or []
                    if getattr(item, "name", "")
                ]
                if names:
                    self._pre_namespace_names[attr] = names

        def _apply_vpc_defaults(self):
            if len(self.vpcs) == 1 and not self.vpcs[0].name:
                self.vpcs[0].name = "vpc"
            default_vpc_name = self.vpcs[0].name if len(self.vpcs) == 1 else ""
            if not default_vpc_name:
                return

            def _apply_pool_defaults(pool):
                if not pool.vpc_name:
                    pool.vpc_name = default_vpc_name
                if (
                    not pool.security_group_ids
                    and not pool.security_group_names
                    and pool.vpc_name
                ):
                    pool.security_group_names = [f"{pool.vpc_name}-sg"]

                launch_template = getattr(pool, "launch_template", None)
                if launch_template:
                    if not getattr(launch_template, "vpc_name", ""):
                        launch_template.vpc_name = pool.vpc_name
                    if (
                        not getattr(launch_template, "security_group_ids", [])
                        and not getattr(launch_template, "security_group_names", [])
                    ):
                        launch_template.security_group_names = list(
                            pool.security_group_names
                        )

                autoscaling_group = getattr(pool, "autoscaling_group", None)
                if autoscaling_group and not getattr(
                    autoscaling_group, "vpc_name", ""
                ):
                    autoscaling_group.vpc_name = pool.vpc_name

            for pool in self.runner_pools:
                _apply_pool_defaults(pool)
            for pool in self.orchestrator_pools:
                _apply_pool_defaults(pool)

            if self.cidb_cluster:
                cluster = self.cidb_cluster
                if not cluster.vpc_name:
                    cluster.vpc_name = default_vpc_name
                if (
                    not cluster.security_group_ids
                    and not cluster.security_group_names
                    and cluster.vpc_name
                ):
                    cluster.security_group_names = [f"{cluster.vpc_name}-sg"]

        def _apply_image_builder_defaults(self):
            default_vpc_name = self.vpcs[0].name if len(self.vpcs) == 1 else ""

            for builder in self.image_builders:
                if not builder.vpc_name and default_vpc_name:
                    builder.vpc_name = default_vpc_name
                if (
                    not builder.security_group_ids
                    and not builder.security_group_names
                    and builder.vpc_name
                ):
                    builder.security_group_names = [f"{builder.vpc_name}-sg"]

        def _apply_project_namespace(self):
            # Centralize namespacing here instead of forcing ci/infrastructure/projects.py
            # to hand-prefix every queue, LT, IAM role, lambda, bucket, etc.
            #
            # This keeps project configs declarative and makes "one config vs
            # many projects" the only user-visible difference. The method
            # mutates every owned component plus any embedded child resources
            # created by higher-level native components such as RunnerPool,
            # OrchestratorPool, PoolAutoscaler, GitHubTokenMinter, and CIDB.
            self._clone_owned_configs()
            self._capture_pre_namespace_names()
            self._apply_vpc_defaults()
            self._apply_image_builder_defaults()

            replacements = {}

            for config in self.vpcs:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)

            for config in self.storages:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)

            for config in self.report_pages:
                if not getattr(config, "bucket_name", "") and self.storages:
                    config.bucket_name = self.storages[0].name
                elif getattr(config, "bucket_name", ""):
                    old_bucket = config.bucket_name
                    config.bucket_name = self._prefixed(config.bucket_name)
                    self._record_rename(replacements, old_bucket, config.bucket_name)

            for config in self.iam_roles:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)

            for config in self.iam_instance_profiles:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                old_role = config.role_name
                config.role_name = self._prefixed(config.role_name)
                self._record_rename(replacements, old_role, config.role_name)

            for config in self.secret_parameters:
                old = config.name
                config.name = self._prefixed_secret_name(config.name)
                self._record_rename(replacements, old, config.name)
                self._record_rename(replacements, old.lstrip("/"), config.name.lstrip("/"))

            for config in self.sqs_queues:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)

            for config in self.launch_templates:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                if config.vpc_name:
                    old_vpc = config.vpc_name
                    config.vpc_name = self._prefixed(config.vpc_name)
                    self._record_rename(replacements, old_vpc, config.vpc_name)
                if config.iam_instance_profile_name:
                    old_profile = config.iam_instance_profile_name
                    config.iam_instance_profile_name = self._prefixed(config.iam_instance_profile_name)
                    self._record_rename(replacements, old_profile, config.iam_instance_profile_name)
                config.security_group_names = [self._prefixed(name) for name in config.security_group_names]
                if config.image_builder_pipeline_name:
                    old_pipeline = config.image_builder_pipeline_name
                    config.image_builder_pipeline_name = self._prefixed(config.image_builder_pipeline_name)
                    self._record_rename(replacements, old_pipeline, config.image_builder_pipeline_name)

            for config in self.autoscaling_groups:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                if config.vpc_name:
                    old_vpc = config.vpc_name
                    config.vpc_name = self._prefixed(config.vpc_name)
                    self._record_rename(replacements, old_vpc, config.vpc_name)
                if config.launch_template_name:
                    old_lt = config.launch_template_name
                    config.launch_template_name = self._prefixed(config.launch_template_name)
                    self._record_rename(replacements, old_lt, config.launch_template_name)

            for config in self.lambda_functions:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                if config.role_name:
                    old_role = config.role_name
                    config.role_name = self._prefixed(config.role_name)
                    self._record_rename(replacements, old_role, config.role_name)
                config.secrets = {
                    self._prefixed_secret_name(secret_name): env_name
                    for secret_name, env_name in config.secrets.items()
                }

            for config in self.image_builders:
                old_derived_names = {
                    attr: getattr(config, attr, "")
                    for attr in (
                        "image_recipe_name",
                        "infrastructure_configuration_name",
                        "distribution_configuration_name",
                        "ami_name",
                        "image_pipeline_name",
                    )
                }
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                config.refresh_derived_names()
                for attr, old_value in old_derived_names.items():
                    new_value = getattr(config, attr, "")
                    if old_value and new_value:
                        self._record_rename(replacements, old_value, new_value)
                if config.instance_profile_name:
                    old_profile = config.instance_profile_name
                    config.instance_profile_name = self._prefixed(
                        config.instance_profile_name
                    )
                    self._record_rename(
                        replacements, old_profile, config.instance_profile_name
                    )
                if config.vpc_name:
                    old_vpc = config.vpc_name
                    config.vpc_name = self._prefixed(config.vpc_name)
                    self._record_rename(replacements, old_vpc, config.vpc_name)
                config.security_group_names = [self._prefixed(name) for name in config.security_group_names]
                for component in config.inline_components:
                    if component.get("name"):
                        component["name"] = self._prefixed(component["name"])
            for config in self.dedicated_hosts:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)

            for config in self.ec2_instances:
                old = config.name
                config.name = self._prefixed(config.name)
                self._record_rename(replacements, old, config.name)
                if getattr(config, "iam_instance_profile_name", ""):
                    old_profile = config.iam_instance_profile_name
                    config.iam_instance_profile_name = self._prefixed(config.iam_instance_profile_name)
                    self._record_rename(replacements, old_profile, config.iam_instance_profile_name)

            for pool in self.runner_pools:
                if pool.vpc_name:
                    old_vpc = pool.vpc_name
                    pool.vpc_name = self._prefixed(pool.vpc_name)
                    self._record_rename(replacements, old_vpc, pool.vpc_name)
                pool.security_group_names = [self._prefixed(name) for name in pool.security_group_names]
                new_allowed_ssm_parameters = []
                for param_name in pool.allowed_ssm_parameters:
                    if param_name.startswith("arn:"):
                        new_allowed_ssm_parameters.append(param_name)
                        continue
                    old_param = param_name
                    new_param = self._prefixed_secret_name(param_name)
                    new_allowed_ssm_parameters.append(new_param)
                    self._record_rename(replacements, old_param, new_param)
                    self._record_rename(
                        replacements, old_param.lstrip("/"), new_param.lstrip("/")
                    )
                pool.allowed_ssm_parameters = new_allowed_ssm_parameters
                new_allowed_secrets = []
                for secret_name in pool.allowed_secrets:
                    if secret_name.startswith("arn:"):
                        new_allowed_secrets.append(secret_name)
                        continue
                    old_secret = secret_name
                    new_secret = self._prefixed_secret_name(secret_name)
                    new_allowed_secrets.append(new_secret)
                    self._record_rename(replacements, old_secret, new_secret)
                    self._record_rename(
                        replacements, old_secret.lstrip("/"), new_secret.lstrip("/")
                    )
                pool.allowed_secrets = new_allowed_secrets
                new_allowed_s3_prefixes = []
                for s3_prefix in pool.allowed_s3_prefixes:
                    if not s3_prefix or not s3_prefix.strip():
                        continue
                    if s3_prefix.startswith("arn:"):
                        new_allowed_s3_prefixes.append(s3_prefix)
                        continue
                    scheme = "s3://" if s3_prefix.startswith("s3://") else ""
                    old_s3_prefix = s3_prefix
                    clean_s3_prefix = s3_prefix.removeprefix("s3://").lstrip("/")
                    bucket, separator, key_prefix = clean_s3_prefix.partition("/")
                    new_bucket = self._prefixed(bucket)
                    new_s3_prefix = f"{scheme}{new_bucket}"
                    if separator:
                        new_s3_prefix = f"{new_s3_prefix}/{key_prefix}"
                    new_allowed_s3_prefixes.append(new_s3_prefix)
                    self._record_rename(replacements, old_s3_prefix, new_s3_prefix)
                    self._record_rename(replacements, bucket, new_bucket)
                pool.allowed_s3_prefixes = new_allowed_s3_prefixes
                new_readonly_s3_prefixes = []
                for s3_prefix in pool.allowed_s3_prefixes_readonly:
                    if not s3_prefix or not s3_prefix.strip():
                        continue
                    if s3_prefix.startswith("arn:"):
                        new_readonly_s3_prefixes.append(s3_prefix)
                        continue
                    scheme = "s3://" if s3_prefix.startswith("s3://") else ""
                    old_s3_prefix = s3_prefix
                    clean_s3_prefix = s3_prefix.removeprefix("s3://").lstrip("/")
                    bucket, separator, key_prefix = clean_s3_prefix.partition("/")
                    new_bucket = self._prefixed(bucket)
                    new_s3_prefix = f"{scheme}{new_bucket}"
                    if separator:
                        new_s3_prefix = f"{new_s3_prefix}/{key_prefix}"
                    new_readonly_s3_prefixes.append(new_s3_prefix)
                    self._record_rename(replacements, old_s3_prefix, new_s3_prefix)
                    self._record_rename(replacements, bucket, new_bucket)
                pool.allowed_s3_prefixes_readonly = new_readonly_s3_prefixes
                old_role_name = pool.ec2_role.name
                pool.ec2_role.name = self._prefixed(pool.ec2_role.name)
                self._record_rename(replacements, old_role_name, pool.ec2_role.name)
                old_profile_name = pool.instance_profile.name
                pool.instance_profile.name = self._prefixed(pool.instance_profile.name)
                self._record_rename(replacements, old_profile_name, pool.instance_profile.name)
                old_profile_role = pool.instance_profile.role_name
                pool.instance_profile.role_name = self._prefixed(pool.instance_profile.role_name)
                self._record_rename(replacements, old_profile_role, pool.instance_profile.role_name)
                old_lt = pool.launch_template.name
                pool.launch_template.name = self._prefixed(pool.launch_template.name)
                self._record_rename(replacements, old_lt, pool.launch_template.name)
                if getattr(pool.launch_template, "vpc_name", ""):
                    old_lt_vpc = pool.launch_template.vpc_name
                    pool.launch_template.vpc_name = self._prefixed(
                        pool.launch_template.vpc_name
                    )
                    self._record_rename(
                        replacements, old_lt_vpc, pool.launch_template.vpc_name
                    )
                if getattr(pool.launch_template, "iam_instance_profile_name", ""):
                    old_lt_profile = pool.launch_template.iam_instance_profile_name
                    pool.launch_template.iam_instance_profile_name = self._prefixed(
                        pool.launch_template.iam_instance_profile_name
                    )
                    self._record_rename(
                        replacements,
                        old_lt_profile,
                        pool.launch_template.iam_instance_profile_name,
                    )
                pool.launch_template.security_group_names = [
                    self._prefixed(name)
                    for name in pool.launch_template.security_group_names
                ]
                old_queue = pool.queue.name
                pool.queue.name = self._prefixed(pool.queue.name)
                self._record_rename(replacements, old_queue, pool.queue.name)
                old_asg = pool.autoscaling_group.name
                pool.autoscaling_group.name = self._prefixed(pool.autoscaling_group.name)
                self._record_rename(replacements, old_asg, pool.autoscaling_group.name)
                if getattr(pool.autoscaling_group, "vpc_name", ""):
                    old_asg_vpc = pool.autoscaling_group.vpc_name
                    pool.autoscaling_group.vpc_name = self._prefixed(
                        pool.autoscaling_group.vpc_name
                    )
                    self._record_rename(
                        replacements, old_asg_vpc, pool.autoscaling_group.vpc_name
                    )
                old_lt_ref = pool.autoscaling_group.launch_template_name
                pool.autoscaling_group.launch_template_name = self._prefixed(pool.autoscaling_group.launch_template_name)
                self._record_rename(replacements, old_lt_ref, pool.autoscaling_group.launch_template_name)

            for pool in self.orchestrator_pools:
                if pool.vpc_name:
                    old_vpc = pool.vpc_name
                    pool.vpc_name = self._prefixed(pool.vpc_name)
                    self._record_rename(replacements, old_vpc, pool.vpc_name)
                if pool.iam_instance_profile_name:
                    old_profile = pool.iam_instance_profile_name
                    pool.iam_instance_profile_name = self._prefixed(pool.iam_instance_profile_name)
                    self._record_rename(replacements, old_profile, pool.iam_instance_profile_name)
                if pool.ec2_role_name:
                    old_role = pool.ec2_role_name
                    pool.ec2_role_name = self._prefixed(pool.ec2_role_name)
                    self._record_rename(replacements, old_role, pool.ec2_role_name)
                pool.security_group_names = [self._prefixed(name) for name in pool.security_group_names]
                old_role_name = pool.ec2_role.name
                pool.ec2_role.name = self._prefixed(pool.ec2_role.name)
                self._record_rename(replacements, old_role_name, pool.ec2_role.name)
                old_profile_name = pool.instance_profile.name
                pool.instance_profile.name = self._prefixed(pool.instance_profile.name)
                self._record_rename(replacements, old_profile_name, pool.instance_profile.name)
                old_profile_role = pool.instance_profile.role_name
                pool.instance_profile.role_name = self._prefixed(pool.instance_profile.role_name)
                self._record_rename(replacements, old_profile_role, pool.instance_profile.role_name)
                old_lt = pool.launch_template.name
                pool.launch_template.name = self._prefixed(pool.launch_template.name)
                self._record_rename(replacements, old_lt, pool.launch_template.name)
                if getattr(pool.launch_template, "vpc_name", ""):
                    old_lt_vpc = pool.launch_template.vpc_name
                    pool.launch_template.vpc_name = self._prefixed(
                        pool.launch_template.vpc_name
                    )
                    self._record_rename(
                        replacements, old_lt_vpc, pool.launch_template.vpc_name
                    )
                if getattr(pool.launch_template, "iam_instance_profile_name", ""):
                    old_lt_profile = pool.launch_template.iam_instance_profile_name
                    pool.launch_template.iam_instance_profile_name = self._prefixed(
                        pool.launch_template.iam_instance_profile_name
                    )
                    self._record_rename(
                        replacements,
                        old_lt_profile,
                        pool.launch_template.iam_instance_profile_name,
                    )
                pool.launch_template.security_group_names = [
                    self._prefixed(name)
                    for name in pool.launch_template.security_group_names
                ]
                old_queue = pool.queue.name
                pool.queue.name = self._prefixed(pool.queue.name)
                self._record_rename(replacements, old_queue, pool.queue.name)
                old_asg = pool.autoscaling_group.name
                pool.autoscaling_group.name = self._prefixed(pool.autoscaling_group.name)
                self._record_rename(replacements, old_asg, pool.autoscaling_group.name)
                if getattr(pool.autoscaling_group, "vpc_name", ""):
                    old_asg_vpc = pool.autoscaling_group.vpc_name
                    pool.autoscaling_group.vpc_name = self._prefixed(
                        pool.autoscaling_group.vpc_name
                    )
                    self._record_rename(
                        replacements, old_asg_vpc, pool.autoscaling_group.vpc_name
                    )
                old_lt_ref = pool.autoscaling_group.launch_template_name
                pool.autoscaling_group.launch_template_name = self._prefixed(pool.autoscaling_group.launch_template_name)
                self._record_rename(replacements, old_lt_ref, pool.autoscaling_group.launch_template_name)
                old_lambda_name = pool.lambda_config.name
                pool.lambda_config.name = self._prefixed(pool.lambda_config.name)
                self._record_rename(replacements, old_lambda_name, pool.lambda_config.name)
                old_lambda_role = pool.lambda_config.role_name
                pool.lambda_config.role_name = self._prefixed(pool.lambda_config.role_name)
                self._record_rename(replacements, old_lambda_role, pool.lambda_config.role_name)
                old_webhook_secret = pool.webhook_secret.name
                pool.webhook_secret.name = self._prefixed_secret_name(pool.webhook_secret.name)
                self._record_rename(replacements, old_webhook_secret, pool.webhook_secret.name)
                self._record_rename(replacements, old_webhook_secret.lstrip("/"), pool.webhook_secret.name.lstrip("/"))
                old_pool_lambda_role = pool.lambda_role.name
                pool.lambda_role.name = self._prefixed(pool.lambda_role.name)
                self._record_rename(replacements, old_pool_lambda_role, pool.lambda_role.name)

            for autoscaler in self.pool_autoscalers:
                old_name = autoscaler.name
                autoscaler.name = self._prefixed(autoscaler.name)
                self._record_rename(replacements, old_name, autoscaler.name)
                old_role = autoscaler.lambda_role_name
                autoscaler.lambda_role_name = self._prefixed(autoscaler.lambda_role_name)
                self._record_rename(replacements, old_role, autoscaler.lambda_role_name)
                old_lambda_name = autoscaler.lambda_config.name
                autoscaler.lambda_config.name = self._prefixed(autoscaler.lambda_config.name)
                self._record_rename(replacements, old_lambda_name, autoscaler.lambda_config.name)
                old_lambda_role = autoscaler.lambda_config.role_name
                autoscaler.lambda_config.role_name = self._prefixed(autoscaler.lambda_config.role_name)
                self._record_rename(replacements, old_lambda_role, autoscaler.lambda_config.role_name)
                old_embedded_role = autoscaler.lambda_role.name
                autoscaler.lambda_role.name = self._prefixed(autoscaler.lambda_role.name)
                self._record_rename(replacements, old_embedded_role, autoscaler.lambda_role.name)

            for token_minter in self.github_token_minters:
                old_name = token_minter.name
                token_minter.name = self._prefixed(token_minter.name)
                self._record_rename(replacements, old_name, token_minter.name)
                old_role = token_minter.role_name
                token_minter.role_name = self._prefixed(token_minter.role_name)
                self._record_rename(replacements, old_role, token_minter.role_name)
                old_secret = token_minter.secret_name
                token_minter.secret_name = self._prefixed_secret_name(token_minter.secret_name)
                self._record_rename(replacements, old_secret, token_minter.secret_name)
                self._record_rename(replacements, old_secret.lstrip("/"), token_minter.secret_name.lstrip("/"))
                old_lambda_name = token_minter.lambda_config.name
                token_minter.lambda_config.name = self._prefixed(token_minter.lambda_config.name)
                self._record_rename(replacements, old_lambda_name, token_minter.lambda_config.name)
                old_lambda_role = token_minter.lambda_config.role_name
                token_minter.lambda_config.role_name = self._prefixed(token_minter.lambda_config.role_name)
                self._record_rename(replacements, old_lambda_role, token_minter.lambda_config.role_name)
                old_embedded_role = token_minter.lambda_role.name
                token_minter.lambda_role.name = self._prefixed(token_minter.lambda_role.name)
                self._record_rename(replacements, old_embedded_role, token_minter.lambda_role.name)

            if self.cidb_cluster:
                cluster = self.cidb_cluster
                if cluster.vpc_name:
                    old_vpc = cluster.vpc_name
                    cluster.vpc_name = self._prefixed(cluster.vpc_name)
                    self._record_rename(replacements, old_vpc, cluster.vpc_name)
                if cluster.iam_instance_profile_name:
                    old_profile = cluster.iam_instance_profile_name
                    cluster.iam_instance_profile_name = self._prefixed(cluster.iam_instance_profile_name)
                    self._record_rename(replacements, old_profile, cluster.iam_instance_profile_name)
                if cluster.ec2_role_name:
                    old_role = cluster.ec2_role_name
                    cluster.ec2_role_name = self._prefixed(cluster.ec2_role_name)
                    self._record_rename(replacements, old_role, cluster.ec2_role_name)
                if cluster.admin_password_secret_name:
                    old_secret = cluster.admin_password_secret_name
                    cluster.admin_password_secret_name = self._prefixed_secret_name(cluster.admin_password_secret_name)
                    self._record_rename(replacements, old_secret, cluster.admin_password_secret_name)
                    self._record_rename(replacements, old_secret.lstrip("/"), cluster.admin_password_secret_name.lstrip("/"))
                cluster.security_group_names = [self._prefixed(name) for name in cluster.security_group_names]
                old_role_name = cluster.ec2_role.name
                cluster.ec2_role.name = self._prefixed(cluster.ec2_role.name)
                self._record_rename(replacements, old_role_name, cluster.ec2_role.name)
                old_profile_name = cluster.instance_profile.name
                cluster.instance_profile.name = self._prefixed(cluster.instance_profile.name)
                self._record_rename(replacements, old_profile_name, cluster.instance_profile.name)
                old_profile_role = cluster.instance_profile.role_name
                cluster.instance_profile.role_name = self._prefixed(cluster.instance_profile.role_name)
                self._record_rename(replacements, old_profile_role, cluster.instance_profile.role_name)
                old_secret_name = cluster.admin_password_secret.name
                cluster.admin_password_secret.name = self._prefixed_secret_name(cluster.admin_password_secret.name)
                self._record_rename(replacements, old_secret_name, cluster.admin_password_secret.name)
                self._record_rename(replacements, old_secret_name.lstrip("/"), cluster.admin_password_secret.name.lstrip("/"))
                for instance in cluster.instances:
                    old_instance_name = instance.name
                    instance.name = self._prefixed(instance.name)
                    self._record_rename(replacements, old_instance_name, instance.name)
                    if getattr(instance, "iam_instance_profile_name", ""):
                        old_instance_profile = instance.iam_instance_profile_name
                        instance.iam_instance_profile_name = self._prefixed(instance.iam_instance_profile_name)
                        self._record_rename(replacements, old_instance_profile, instance.iam_instance_profile_name)

            for config in self.iam_roles:
                config.inline_policies = self._replace_recursive(config.inline_policies, replacements)
            for config in self.lambda_functions:
                config.environments = self._replace_recursive(config.environments, replacements)
                config.secrets = self._replace_recursive(config.secrets, replacements)
                config.inline_policies = self._replace_recursive(config.inline_policies, replacements)
            for config in self.launch_templates:
                config.tags = self._replace_recursive(config.tags, replacements)
                config.user_data = self._replace_recursive(config.user_data, replacements)
            for config in self.autoscaling_groups:
                config.tags = self._replace_recursive(config.tags, replacements)
            for config in self.ec2_instances:
                config.user_data = self._replace_recursive(getattr(config, "user_data", ""), replacements)

            # The controller reads this tag as PRAKTIKA_PROJECT_SLUG and builds
            # resource names from it (e.g. "{slug}-gh-token"). It must be the
            # normalized slug that every other AWS resource is named with, not
            # the raw (possibly mixed-case) project name.
            project_slug = self._project_prefix()

            for pool in self.runner_pools:
                pool.launch_template.tags["praktika_project_slug"] = project_slug
                pool.autoscaling_group.tags["praktika_project_slug"] = project_slug
                pool.ec2_role.inline_policies = self._replace_recursive(pool.ec2_role.inline_policies, replacements)
                pool.launch_template.tags = self._replace_recursive(pool.launch_template.tags, replacements)
                pool.launch_template.user_data = self._replace_recursive(pool.launch_template.user_data, replacements)
                pool.autoscaling_group.tags = self._replace_recursive(pool.autoscaling_group.tags, replacements)

            for pool in self.orchestrator_pools:
                pool.launch_template.tags["praktika_project_slug"] = project_slug
                pool.autoscaling_group.tags["praktika_project_slug"] = project_slug
                pool.ec2_role.inline_policies = self._replace_recursive(pool.ec2_role.inline_policies, replacements)
                pool.lambda_role.inline_policies = self._replace_recursive(pool.lambda_role.inline_policies, replacements)
                pool.lambda_config.environments = self._replace_recursive(pool.lambda_config.environments, replacements)
                pool.lambda_config.secrets = self._replace_recursive(pool.lambda_config.secrets, replacements)
                pool.launch_template.tags = self._replace_recursive(pool.launch_template.tags, replacements)
                pool.launch_template.user_data = self._replace_recursive(pool.launch_template.user_data, replacements)
                pool.autoscaling_group.tags = self._replace_recursive(pool.autoscaling_group.tags, replacements)

            for autoscaler in self.pool_autoscalers:
                autoscaler.lambda_role.inline_policies = self._replace_recursive(autoscaler.lambda_role.inline_policies, replacements)
                autoscaler.lambda_config.environments = self._replace_recursive(autoscaler.lambda_config.environments, replacements)

            for token_minter in self.github_token_minters:
                token_minter.lambda_role.inline_policies = self._replace_recursive(token_minter.lambda_role.inline_policies, replacements)
                token_minter.lambda_config.environments = self._replace_recursive(token_minter.lambda_config.environments, replacements)

            if self.cidb_cluster:
                cluster = self.cidb_cluster
                cluster.ec2_role.inline_policies = self._replace_recursive(cluster.ec2_role.inline_policies, replacements)
                for instance in cluster.instances:
                    instance.user_data = self._replace_recursive(getattr(instance, "user_data", ""), replacements)

        def __post_init__(self):
            if self.orchestrator_pool:
                if not self.orchestrator_pools:
                    self.orchestrator_pools = [self.orchestrator_pool]
                elif all(pool.name != self.orchestrator_pool.name for pool in self.orchestrator_pools):
                    self.orchestrator_pools.insert(0, self.orchestrator_pool)
            self.orchestrator_pool = (
                self.orchestrator_pools[0] if self.orchestrator_pools else None
            )
            for token_minter in self.github_token_minters:
                token_minter.apply_defaults(default_repository=self.name)
            default_allowed_repositories = []
            for token_minter in self.github_token_minters:
                for repo in token_minter.repositories:
                    if repo not in default_allowed_repositories:
                        default_allowed_repositories.append(repo)
            if default_allowed_repositories:
                for pool in self.orchestrator_pools:
                    pool.ext.setdefault(
                        "allowed_repositories",
                        list(default_allowed_repositories),
                    )

            # 1. Namespace all resources for this project.
            # 2. Materialize implicit child components from the high-level
            #    native components (pools, token minters, CIDB, autoscaler).
            #
            # Ordering matters: the implicit children must be created after
            # namespacing so their names and cross-references land in the same
            # project namespace as the rest of the config.
            self._apply_project_namespace()
            self.orchestrator_pool = (
                self.orchestrator_pools[0] if self.orchestrator_pools else None
            )
            seen_role_names: set = {r.name for r in self.iam_roles}
            seen_profile_names: set = {p.name for p in self.iam_instance_profiles}
            seen_secret_names: set = {s.name for s in self.secret_parameters}

            def _add_role(role):
                if role.name not in seen_role_names:
                    self.iam_roles.append(role)
                    seen_role_names.add(role.name)

            def _add_profile(profile):
                if profile.name not in seen_profile_names:
                    self.iam_instance_profiles.append(profile)
                    seen_profile_names.add(profile.name)

            def _add_secret(secret):
                if secret.name not in seen_secret_names:
                    self.secret_parameters.append(secret)
                    seen_secret_names.add(secret.name)

            image_builder_role = IAMRole.Config(
                name=self._prefixed("imagebuilder-role"),
                trust_service="ec2.amazonaws.com",
                policy_arns=[
                    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
                    "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
                    "arn:aws:iam::aws:policy/EC2InstanceProfileForImageBuilder",
                ],
            )
            image_builder_profile = IAMInstanceProfile.Config(
                name=self._prefixed("imagebuilder-profile"),
                role_name=image_builder_role.name,
            )
            needs_image_builder_profile = False
            for builder in self.image_builders:
                if (
                    builder.instance_profile_name
                    or builder.infrastructure_configuration
                ):
                    continue
                builder.instance_profile_name = image_builder_profile.name
                needs_image_builder_profile = True
            if needs_image_builder_profile:
                _add_role(image_builder_role)
                _add_profile(image_builder_profile)

            implicit_autoscaler_sources = []
            for pool in self.orchestrator_pools:
                _add_secret(pool.webhook_secret)
                _add_role(pool.ec2_role)
                _add_role(pool.lambda_role)
                _add_profile(pool.instance_profile)
                self.lambda_functions.append(pool.lambda_config)
                self.sqs_queues.append(pool.queue)
                self.launch_templates.append(pool.launch_template)
                self.autoscaling_groups.append(pool.autoscaling_group)
                implicit_autoscaler_sources.append(pool)
            for pool in self.runner_pools:
                _add_role(pool.ec2_role)
                _add_profile(pool.instance_profile)
                self.launch_templates.append(pool.launch_template)
                self.autoscaling_groups.append(pool.autoscaling_group)
                self.sqs_queues.append(pool.queue)
                implicit_autoscaler_sources.append(pool)
            from .native.pool_autoscaler import PoolAutoscaler as _PoolAutoscaler
            implicit_runner_autoscaler = _PoolAutoscaler.from_pools(
                implicit_autoscaler_sources,
                interval_seconds=self.pool_autoscaler_interval_seconds,
            )
            if implicit_runner_autoscaler:
                implicit_runner_autoscaler.name = self._prefixed(
                    implicit_runner_autoscaler.name
                )
                implicit_runner_autoscaler.lambda_role_name = self._prefixed(
                    implicit_runner_autoscaler.lambda_role_name
                )
                implicit_runner_autoscaler.lambda_config.name = self._prefixed(
                    implicit_runner_autoscaler.lambda_config.name
                )
                implicit_runner_autoscaler.lambda_config.role_name = self._prefixed(
                    implicit_runner_autoscaler.lambda_config.role_name
                )
                implicit_runner_autoscaler.lambda_role.name = self._prefixed(
                    implicit_runner_autoscaler.lambda_role.name
                )
            if implicit_runner_autoscaler and not any(
                autoscaler.name == implicit_runner_autoscaler.name
                for autoscaler in self.pool_autoscalers
            ):
                self.pool_autoscalers.append(implicit_runner_autoscaler)
            for token_minter in self.github_token_minters:
                _add_role(token_minter.lambda_role)
                self.lambda_functions.append(token_minter.lambda_config)
                for pool in self.orchestrator_pools:
                    token_minter.grant_invoke(pool.ec2_role)
                    token_minter.grant_invoke(pool.lambda_role)
                for pool in self.runner_pools:
                    token_minter.grant_invoke(pool.ec2_role)
            for autoscaler in self.pool_autoscalers:
                _add_role(autoscaler.lambda_role)
                self.lambda_functions.append(autoscaler.lambda_config)

            if self.cidb_cluster:
                # CIDB instance launches happen via CIDBCluster.deploy() (it
                # also authorizes SG ingress); only the supporting IAM/secret
                # are registered here so they roll out in the standard order.
                _add_role(self.cidb_cluster.ec2_role)
                _add_profile(self.cidb_cluster.instance_profile)
                self.secret_parameters.append(self.cidb_cluster.admin_password_secret)

        def _verify_account(self):
            from botocore.exceptions import (
                BotoCoreError,
                ClientError,
                NoCredentialsError,
                ProfileNotFound,
            )

            from ._utils import aws_account_id

            profile = self._settings.AWS_PROFILE or "<default>"
            try:
                actual = aws_account_id(self._settings.AWS_REGION)
            except ProfileNotFound as e:
                raise SystemExit(
                    f"AWS profile [{profile}] not found: {e}. "
                    f"Configure it in ~/.aws/config or update Settings.AWS_PROFILE."
                )
            except NoCredentialsError:
                raise SystemExit(
                    f"No AWS credentials available for profile [{profile}]. "
                    f"Run: aws sso login --profile {profile}"
                )
            except (ClientError, BotoCoreError) as e:
                msg = str(e)
                if (
                    "UnauthorizedSSOTokenError" in type(e).__name__
                    or "expired" in msg.lower()
                    or "Session token not found or invalid" in msg
                    or "InvalidGrantException" in msg
                ):
                    raise SystemExit(
                        f"AWS SSO session for profile [{profile}] is expired or invalid. "
                        f"Run: aws sso login --profile {profile}"
                    )
                raise SystemExit(
                    f"AWS auth check failed for profile [{profile}]: {e}"
                )
            configured = (self._settings.AWS_ACCOUNT_ID or "").strip()
            if configured and actual != configured:
                raise RuntimeError(
                    f"AWS account mismatch: configured={configured}, "
                    f"actual={actual}. Aborting to prevent accidental changes to the wrong account."
                )
            if configured:
                print(f"AWS account verified: {actual} (profile: {profile})")
            else:
                print(
                    f"AWS account resolved from credentials: {actual} "
                    f"(profile: {profile}). No configured AWS_ACCOUNT_ID check."
                )

        def _validate_min_praktika_version(self):
            current = current_praktika_version()
            required = str(self.min_praktika_version or "0.0.0")
            try:
                required_key = version_key(required)
                current_key = version_key(current)
            except (TypeError, ValueError):
                raise SystemExit(
                    "Invalid infrastructure config version.\n"
                    f"Project [{self.name}] sets min_praktika_version="
                    f"{self.min_praktika_version!r}, but it must be a dotted "
                    "numeric package version such as '0.1.2'.\n"
                    f"Running Praktika version is {current}."
                )

            if required_key <= current_key:
                return

            raise SystemExit(
                "Infrastructure config requires a newer Praktika runtime.\n"
                f"Project: {self.name}\n"
                f"Config min_praktika_version: {required}\n"
                f"Running Praktika version: {current}\n"
                "Newer Praktika versions are expected to support older configs, "
                "but older Praktika versions cannot safely deploy configs that "
                "use newer infrastructure fields or semantics.\n"
                "Use the Praktika checkout/package that matches this config, "
                "for example: python3 -m praktika infrastructure --deploy ..."
            )

        def _deployment_warnings(self, autoscaling_groups=None) -> List[str]:
            warnings = []
            for asg_config in autoscaling_groups or []:
                if not asg_config.ext.get("deferred_missing_launch_template"):
                    continue
                warning = asg_config.ext.get("deployment_warning")
                if not warning:
                    warning = (
                        f"Launch Template is not available yet for ASG "
                        f"'{asg_config.name}'; skipping until the launch template exists"
                    )
                warnings.append(warning)
            return warnings

        def _print_deployment_warnings(self, autoscaling_groups=None):
            warnings = self._deployment_warnings(autoscaling_groups)
            if not warnings:
                return

            print("\n" + "=" * 60)
            print("WARNING: Infrastructure deployment completed with warnings")
            print("=" * 60)
            for warning in warnings:
                print(f"WARNING: {warning}")
            print(
                "WARNING: Rerun is required after the missing launch template exists."
            )

        def deploy(
            self,
            all=False,
            only: Optional[List[str]] = None,
            is_test: bool = False,
        ):
            """
            Deploy Lambda functions.

            Args:
                all: If False, only deploy code (skip settings validation and IAM policies).
                     If True, deploy everything (validate settings, deploy code, attach IAM policies).
                only: If set, deploy only selected component types by name.
            """
            self._validate_min_praktika_version()
            self._verify_account()

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
                and not self.secret_parameters
                and not self.iam_roles
                and not self.iam_instance_profiles
                and not self.dedicated_hosts
                and not self.ec2_instances
                and not self.image_builders
                and not self.launch_templates
                and not self.autoscaling_groups
                and not self.sqs_queues
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
                    # Only fall back to the global region setting when no explicit AZs are
                    # configured; otherwise _resolved_region() derives the correct region
                    # from the AZ name and a single AWS_REGION would break multi-region setups.
                    if (
                        self._settings
                        and self._settings.AWS_REGION
                        and not host_config.availability_zones
                    ):
                        host_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Dedicated Hosts: {host_config.name}")
                    print("=" * 60)
                    host_config.deploy()

            # Deploy IAM roles before anything that depends on them
            if _wants("IAMRole", "IAMRoles"):
                for role_config in self.iam_roles:
                    role_config.region = self._settings.AWS_REGION
                    print("\n" + "=" * 60)
                    print(f"Deploying IAM Role: {role_config.name}")
                    print("=" * 60)
                    role_config.deploy()

            # Deploy IAM Instance Profiles (role must exist first)
            if _wants("IAMInstanceProfile", "IAMInstanceProfiles"):
                for ip_config in self.iam_instance_profiles:
                    ip_config.region = self._settings.AWS_REGION
                    print("\n" + "=" * 60)
                    print(f"Deploying IAM Instance Profile: {ip_config.name}")
                    print("=" * 60)
                    ip_config.deploy()

            # Deploy EC2 Instances
            if _wants("EC2Instance", "EC2Instances", "Instance", "Instances"):
                for instance_config in self.ec2_instances:
                    instance_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying EC2 Instance: {instance_config.name}")
                    print("=" * 60)
                    instance_config.deploy()

            # Deploy VPCs (before ASGs that reference them by name)
            if _wants("VPC", "VPCs"):
                for vpc_config in self.vpcs:
                    vpc_config.region = self._settings.AWS_REGION
                    print("\n" + "=" * 60)
                    print(f"Deploying VPC: {vpc_config.name}")
                    print("=" * 60)
                    vpc_config.deploy()

            # Deploy secret parameters (before Lambdas that reference them)
            if _wants("SecretParameter", "SecretParameters", "Secret", "Secrets"):
                for secret_config in self.secret_parameters:
                    secret_config.region = self._settings.AWS_REGION
                    print("\n" + "=" * 60)
                    print(f"Deploying Secret Parameter: {secret_config.name}")
                    print("=" * 60)
                    secret_config.deploy()

            # Deploy CI DB cluster: authorizes SG ingress for ClickHouse ports
            # and launches each replica EC2. Runs after SecretParameter so the
            # admin password is in SSM before user_data fetches it.
            if (
                _wants("CIDBCluster", "CIDB", "CIDBClusters", "CI_DB")
                and self.cidb_cluster
            ):
                self.cidb_cluster.region = self._settings.AWS_REGION
                print("\n" + "=" * 60)
                print(f"Deploying CIDB Cluster (size={self.cidb_cluster.size})")
                print("=" * 60)
                self.cidb_cluster.deploy()

            # Deploy S3 buckets
            if _wants("Storage", "Storages", "S3", "Bucket", "Buckets"):
                for storage_config in self.storages:
                    storage_config.region = self._settings.AWS_REGION
                    print("\n" + "=" * 60)
                    print(f"Deploying Storage: {storage_config.name}")
                    print("=" * 60)
                    storage_config.deploy()

            # Upload HTML report pages (after Storage so the bucket exists)
            if _wants("ReportPage", "ReportPages", "Html", "HTML"):
                for rp in self.report_pages:
                    print("\n" + "=" * 60)
                    print(f"Deploying ReportPage: {rp.path}")
                    print("=" * 60)
                    rp.deploy(is_test=is_test)

            # Deploy SQS queues (before Lambdas so queue URLs are available)
            if _wants("SQS", "SQSQueue", "SQSQueues"):
                for sqs_config in self.sqs_queues:
                    sqs_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying SQS Queue: {sqs_config.name}")
                    print("=" * 60)
                    sqs_config.deploy()

            # Deploy all Lambdas before image-backed compute so webhook/API
            # entrypoints are recreated even while Image Builder pipelines are
            # still producing their first ready AMIs.
            if _wants("Lambda", "Lambdas", "LambdaFunction", "LambdaFunctions"):
                for lambda_config in self.lambda_functions:
                    # Always set region if available (needed even for code-only deploys)
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

            # Deploy Image Builder pipelines after Lambdas. The webhook/API
            # surface should be available even if compute images are still
            # building.
            if _wants("ImageBuilder", "ImageBuilders"):
                for ib_config in self.image_builders:
                    ib_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Image Builder: {ib_config.name}")
                    print("=" * 60)
                    ib_config.deploy()

            # Deploy all Launch Templates after ImageBuilders so image-builder
            # backed templates can resolve their latest AMI ids.
            if _wants("LaunchTemplate", "LaunchTemplates"):
                for lt_config in self.launch_templates:
                    lt_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Launch Template: {lt_config.name}")
                    print("=" * 60)
                    lt_config.deploy()

            # Deploy all ASGs after Launch Templates.
            deployed_asg_configs = []
            if _wants("AutoScalingGroup", "AutoScalingGroups", "ASG", "ASGs"):
                for asg_config in self.autoscaling_groups:
                    asg_config.region = self._settings.AWS_REGION

                    print("\n" + "=" * 60)
                    print(f"Deploying Auto Scaling Group: {asg_config.name}")
                    print("=" * 60)
                    asg_config.deploy()
                    deployed_asg_configs.append(asg_config)

            self._print_deployment_warnings(deployed_asg_configs)

        def destroy_runtime(
            self,
            force: bool = True,
            only: Optional[List[str]] = None,
        ):
            """
            Delete project-prefixed execution-plane resources that can be
            recreated by deploy. Preserve stateful resources and scarce
            capacity allocations.

            Args:
                force: If True, forcefully terminate instances without stopping first.
                only: If set, destroy only selected runtime component types by name.
            """
            self._verify_account()
            self._destroy_by_prefix(include_all=False, force=force, only=only)

        def destroy_all(self, only: Optional[List[str]] = None):
            """
            Delete all project-prefixed managed infrastructure resources.

            This intentionally discovers resources from AWS by project slug
            prefix instead of trusting the current config object graph.
            """
            self._verify_account()
            self._destroy_by_prefix(include_all=True, force=True, only=only)

        def _destroy_by_prefix(
            self,
            *,
            include_all: bool,
            force: bool,
            only: Optional[List[str]],
        ):
            from ..interactive import UserPrompt

            region = self._settings.AWS_REGION
            prefix = self._project_prefix()
            prefix_dash = f"{prefix}-"
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

            def _iter_pages(client, operation: str, **kwargs):
                paginator = getattr(client, "get_paginator", None)
                if paginator:
                    try:
                        for page in paginator(operation).paginate(**kwargs):
                            yield page
                        return
                    except Exception as e:
                        if e.__class__.__name__ != "OperationNotPageableError":
                            raise

                def _operation_input_members():
                    meta = getattr(client, "meta", None)
                    mapping = getattr(meta, "method_to_api_mapping", {}) or {}
                    operation_name = mapping.get(operation)
                    service_model = getattr(meta, "service_model", None)
                    if not operation_name or service_model is None:
                        return set()
                    try:
                        operation_model = service_model.operation_model(operation_name)
                    except Exception:
                        return set()
                    input_shape = getattr(operation_model, "input_shape", None)
                    return set(getattr(input_shape, "members", {}) or {})

                def _request_token_key() -> str:
                    input_members = _operation_input_members()
                    for candidate in (
                        "NextToken",
                        "nextToken",
                        "Marker",
                        "marker",
                        "ContinuationToken",
                    ):
                        if candidate in input_members:
                            return candidate
                    return "NextToken"

                def _response_token(page) -> str:
                    for candidate in (
                        "NextToken",
                        "nextToken",
                        "NextMarker",
                        "nextMarker",
                        "Marker",
                        "marker",
                        "NextContinuationToken",
                    ):
                        token = page.get(candidate)
                        if token:
                            return token
                    return ""

                method = getattr(client, operation)
                token = ""
                token_key = _request_token_key()
                while True:
                    request = dict(kwargs)
                    if token:
                        request[token_key] = token
                    page = method(**request)
                    yield page
                    token = _response_token(page)
                    if not token:
                        break

            def _resource_name_from_tags(tags) -> str:
                for tag in tags or []:
                    if tag.get("Key") == "Name":
                        return tag.get("Value", "")
                return ""

            def _tag_value(tags, key: str) -> str:
                for tag in tags or []:
                    if tag.get("Key") == key:
                        return tag.get("Value", "")
                return ""

            def _is_prefix_name(name: str) -> bool:
                return bool(name and name.startswith(prefix_dash))

            def _ignore_missing(fn, *args, ignore_dependency: bool = False, **kwargs):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    message = str(e).lower()
                    missing = (
                        "not found" in message
                        or "does not exist" in message
                        or "nonexistent" in message
                        or e.__class__.__name__
                        in {
                            "NoSuchEntityException",
                            "ResourceNotFoundException",
                            "QueueDoesNotExist",
                            "ParameterNotFound",
                        }
                    )
                    dependency = "dependency" in message or "in use" in message
                    if missing or (ignore_dependency and dependency):
                        print(f"Skipped missing/dependent resource: {e}")
                        return None
                    raise

            def _confirm_and_run(label: str, fn):
                print("\n" + "=" * 60)
                print(f"Destroy: {label}")
                print("=" * 60)
                if UserPrompt.confirm(f"Delete '{label}'?"):
                    fn()
                    return True
                else:
                    print("Skipped.")
                    return False

            def _confirm_batch_and_run(
                label: str,
                item_labels: List[str],
                fn,
                prompt_label: Optional[str] = None,
            ):
                print("\n" + "=" * 60)
                print(f"Destroy: {label}")
                print("=" * 60)
                for item_label in item_labels:
                    print(f"  - {item_label}")
                label_for_prompt = prompt_label or label
                if UserPrompt.confirm(
                    f"Delete all {len(item_labels)} {label_for_prompt}?"
                ):
                    fn()
                    return True
                print("Skipped.")
                return False

            def _api_gateway_names() -> Dict[str, str]:
                apigw = aws_client("apigatewayv2", region, f"{prefix}-api-discovery")
                api_by_lambda: Dict[str, str] = {}
                for page in _iter_pages(apigw, "get_apis"):
                    for api in page.get("Items", []) or []:
                        name = api.get("Name", "")
                        api_id = api.get("ApiId", "")
                        if _is_prefix_name(name) and name.endswith("-API") and api_id:
                            api_by_lambda[name[:-4]] = api_id
                return api_by_lambda

            def _lambda_names() -> List[str]:
                client = aws_client("lambda", region, f"{prefix}-lambda-discovery")
                names = []
                for page in _iter_pages(client, "list_functions"):
                    for item in page.get("Functions", []) or []:
                        name = item.get("FunctionName", "")
                        if _is_prefix_name(name):
                            names.append(name)
                return sorted(set(names))

            def _api_gateway_lambda_names() -> set:
                return set(_api_gateway_names().keys())

            api_gateway_lambda_names = set() if include_all else _api_gateway_lambda_names()
            protected_role_names = set()
            if api_gateway_lambda_names:
                lambda_client = aws_client("lambda", region, f"{prefix}-lambda-protect")
                for lambda_name in api_gateway_lambda_names:
                    try:
                        role_arn = lambda_client.get_function(
                            FunctionName=lambda_name
                        )["Configuration"].get("Role", "")
                    except Exception as e:
                        print(
                            f"Warning: failed to resolve role for protected Lambda {lambda_name}: {e}"
                        )
                        continue
                    role_name = role_arn.split("/")[-1] if role_arn else ""
                    if role_name:
                        protected_role_names.add(role_name)
            if not include_all:
                protected_role_names.update(
                    {
                        f"{prefix_dash}cidb-role",
                        f"{prefix_dash}cidb-profile",
                    }
                )

            did_work = False
            asg_managed_instance_ids = set()

            if _wants("AutoScalingGroup", "AutoScalingGroups", "ASG", "ASGs"):
                client = aws_client("autoscaling", region, f"{prefix}-asg-destroy")
                instance_ids_by_asg = {}
                for page in _iter_pages(client, "describe_auto_scaling_groups"):
                    for item in page.get("AutoScalingGroups", []) or []:
                        name = item.get("AutoScalingGroupName", "")
                        if _is_prefix_name(name):
                            instance_ids_by_asg[name] = {
                                instance.get("InstanceId")
                                for instance in item.get("Instances", []) or []
                                if instance.get("InstanceId")
                            }
                for name in sorted(instance_ids_by_asg):
                    did_work = True
                    deleted = _confirm_and_run(
                        f"AutoScalingGroup {name}",
                        lambda n=name: _ignore_missing(
                            client.delete_auto_scaling_group,
                            AutoScalingGroupName=n,
                            ForceDelete=True,
                        ),
                    )
                    if deleted:
                        asg_managed_instance_ids.update(instance_ids_by_asg[name])

            if include_all and _wants(
                "EC2Instance", "EC2Instances", "Instance", "Instances"
            ):
                client = aws_client("ec2", region, f"{prefix}-ec2-destroy")
                instances = []
                for page in _iter_pages(
                    client,
                    "describe_instances",
                    Filters=[
                        {
                            "Name": "instance-state-name",
                            "Values": [
                                "pending",
                                "running",
                                "stopping",
                                "stopped",
                            ],
                        }
                    ],
                ):
                    for reservation in page.get("Reservations", []) or []:
                        for item in reservation.get("Instances", []) or []:
                            instance_id = item.get("InstanceId", "")
                            tags = item.get("Tags", []) or []
                            name = _resource_name_from_tags(tags)
                            rn = _tag_value(tags, "praktika_rn")
                            if (
                                instance_id
                                and instance_id not in asg_managed_instance_ids
                                and (_is_prefix_name(name) or _is_prefix_name(rn))
                            ):
                                instances.append((instance_id, name or rn or instance_id))
                for instance_id, name in sorted(set(instances), key=lambda x: x[1]):
                    did_work = True
                    _confirm_and_run(
                        f"EC2Instance {name} ({instance_id})",
                        lambda iid=instance_id: _ignore_missing(
                            client.terminate_instances,
                            InstanceIds=[iid],
                        ),
                    )

            if _wants("LaunchTemplate", "LaunchTemplates"):
                client = aws_client("ec2", region, f"{prefix}-lt-destroy")
                names = []
                for page in _iter_pages(client, "describe_launch_templates"):
                    for item in page.get("LaunchTemplates", []) or []:
                        name = item.get("LaunchTemplateName", "")
                        if _is_prefix_name(name):
                            names.append(name)
                for name in sorted(set(names)):
                    did_work = True
                    _confirm_and_run(
                        f"LaunchTemplate {name}",
                        lambda n=name: _ignore_missing(
                            client.delete_launch_template,
                            LaunchTemplateName=n,
                        ),
                    )

            if _wants("SQS", "SQSQueue", "SQSQueues"):
                client = aws_client("sqs", region, f"{prefix}-sqs-destroy")
                urls = []
                response = client.list_queues(QueueNamePrefix=prefix_dash)
                urls.extend(response.get("QueueUrls", []) or [])
                queue_items = [
                    (url.rstrip("/").split("/")[-1], url)
                    for url in sorted(set(urls))
                ]
                if queue_items:
                    did_work = True

                    def _delete_sqs_queues():
                        for _, url in queue_items:
                            _ignore_missing(client.delete_queue, QueueUrl=url)

                    _confirm_batch_and_run(
                        "SQSQueues",
                        [name for name, _ in queue_items],
                        _delete_sqs_queues,
                        prompt_label="SQS queues",
                    )

            if _wants(
                "EventBridgeSchedule",
                "EventBridgeSchedules",
                "Schedule",
                "Schedules",
                "EventRule",
                "EventRules",
            ):
                client = aws_client("events", region, f"{prefix}-events-destroy")
                rules = []
                for page in _iter_pages(client, "list_rules", NamePrefix=prefix_dash):
                    for item in page.get("Rules", []) or []:
                        name = item.get("Name", "")
                        if _is_prefix_name(name):
                            rules.append(name)
                for name in sorted(set(rules)):
                    did_work = True

                    def _delete_rule(rule_name=name):
                        target_ids = []
                        for page in _iter_pages(
                            client,
                            "list_targets_by_rule",
                            Rule=rule_name,
                        ):
                            for target in page.get("Targets", []) or []:
                                target_id = target.get("Id", "")
                                if target_id:
                                    target_ids.append(target_id)
                        if target_ids:
                            _ignore_missing(
                                client.remove_targets,
                                Rule=rule_name,
                                Ids=target_ids,
                                Force=True,
                            )
                        _ignore_missing(client.delete_rule, Name=rule_name, Force=True)

                    _confirm_and_run(f"EventBridgeSchedule {name}", _delete_rule)

            if _wants(
                "RuntimeLambda",
                "RuntimeLambdas",
                "Lambda",
                "Lambdas",
                "LambdaFunction",
                "LambdaFunctions",
            ):
                client = aws_client("lambda", region, f"{prefix}-lambda-destroy")
                for name in _lambda_names():
                    did_work = True
                    _confirm_and_run(
                        f"Lambda {name}",
                        lambda n=name: _ignore_missing(
                            client.delete_function,
                            FunctionName=n,
                        ),
                    )

            if include_all and _wants(
                "APIGateway",
                "APIGateways",
                "API",
                "APIs",
                "Webhook",
                "Webhooks",
            ):
                client = aws_client("apigatewayv2", region, f"{prefix}-api-destroy")
                for lambda_name, api_id in sorted(_api_gateway_names().items()):
                    did_work = True
                    _confirm_and_run(
                        f"APIGateway {lambda_name}-API",
                        lambda aid=api_id: _ignore_missing(
                            client.delete_api,
                            ApiId=aid,
                        ),
                    )

            def _delete_project_amis():
                nonlocal did_work

                ec2 = aws_client("ec2", region, f"{prefix}-ami-destroy")
                targets = []
                for page in _iter_pages(
                    ec2,
                    "describe_images",
                    Owners=["self"],
                    Filters=[{"Name": "name", "Values": [f"{prefix_dash}*"]}],
                ):
                    for image in page.get("Images", []) or []:
                        image_id = image.get("ImageId", "")
                        name = image.get("Name", "")
                        tags = image.get("Tags", []) or []
                        tag_name = _resource_name_from_tags(tags)
                        rn = _tag_value(tags, "praktika_rn")
                        if not image_id or not (
                            _is_prefix_name(name)
                            or _is_prefix_name(tag_name)
                            or _is_prefix_name(rn)
                        ):
                            continue
                        snapshot_ids = []
                        for mapping in image.get("BlockDeviceMappings", []) or []:
                            snapshot_id = (
                                mapping.get("Ebs", {}) or {}
                            ).get("SnapshotId", "")
                            if snapshot_id:
                                snapshot_ids.append(snapshot_id)
                        targets.append(
                            (
                                f"AMI {name or tag_name or rn or image_id}",
                                image_id,
                                tuple(sorted(set(snapshot_ids))),
                            )
                        )

                for target_label, image_id, snapshot_ids in sorted(
                    set(targets), key=lambda x: x[0]
                ):
                    did_work = True

                    def _delete_image(
                        iid=image_id,
                        sids=snapshot_ids,
                    ):
                        _ignore_missing(
                            ec2.deregister_image,
                            ImageId=iid,
                            ignore_dependency=True,
                        )
                        for snapshot_id in sids:
                            _ignore_missing(
                                ec2.delete_snapshot,
                                SnapshotId=snapshot_id,
                                ignore_dependency=True,
                            )

                    _confirm_and_run(target_label, _delete_image)

            imagebuilder_wanted = _wants("ImageBuilder", "ImageBuilders")
            ami_wanted = imagebuilder_wanted or _wants(
                "AMI",
                "AMIs",
                "Image",
                "Images",
                "EC2Image",
                "EC2Images",
            )

            if imagebuilder_wanted:
                client = aws_client("imagebuilder", region, f"{prefix}-ib-destroy")

                def _delete_imagebuilder_resources(
                    list_op: str,
                    result_key: str,
                    arn_arg: str,
                    delete_op: str,
                    label: str,
                    *,
                    owner_self: bool = False,
                    ignore_dependency: bool = False,
                    group_versions: bool = False,
                ):
                    nonlocal did_work

                    def _item_version(item) -> str:
                        version = (
                            item.get("semanticVersion")
                            or item.get("version")
                            or ""
                        )
                        arn = item.get("arn", "")
                        if not version and "/" in arn:
                            version = arn.rstrip("/").rsplit("/", 1)[-1]
                        return version

                    def _item_label(item, name: str) -> str:
                        version = _item_version(item)
                        if version and version != name:
                            return f"{label} {name} ({version})"
                        return f"{label} {name}"

                    request = {"owner": "Self"} if owner_self else {}
                    targets = []
                    targets_by_name: Dict[str, List[tuple]] = {}
                    for page in _iter_pages(client, list_op, **request):
                        for item in page.get(result_key, []) or []:
                            name = item.get("name", "")
                            arn = item.get("arn", "")
                            if _is_prefix_name(name) and arn:
                                if group_versions:
                                    targets_by_name.setdefault(
                                        f"{label} {name}", []
                                    ).append((_item_version(item), arn))
                                else:
                                    targets.append((_item_label(item, name), arn))
                    if group_versions:
                        for target_label, items in sorted(targets_by_name.items()):
                            unique_items = sorted(set(items), key=lambda x: x[0])
                            did_work = True
                            if len(unique_items) == 1:
                                version, arn = unique_items[0]
                                item_label = (
                                    f"{target_label} ({version})"
                                    if version
                                    else target_label
                                )
                                _confirm_and_run(
                                    item_label,
                                    lambda a=arn: _ignore_missing(
                                        getattr(client, delete_op),
                                        **{arn_arg: a},
                                        ignore_dependency=ignore_dependency,
                                    ),
                                )
                                continue

                            def _delete_versions(arns=[arn for _, arn in unique_items]):
                                for a in arns:
                                    _ignore_missing(
                                        getattr(client, delete_op),
                                        **{arn_arg: a},
                                        ignore_dependency=ignore_dependency,
                                    )

                            _confirm_batch_and_run(
                                target_label,
                                [version or arn for version, arn in unique_items],
                                _delete_versions,
                                prompt_label=f"versions of {target_label}",
                            )
                        return

                    for target_label, arn in sorted(set(targets), key=lambda x: x[0]):
                        did_work = True
                        _confirm_and_run(
                            target_label,
                            lambda a=arn: _ignore_missing(
                                getattr(client, delete_op),
                                **{arn_arg: a},
                                ignore_dependency=ignore_dependency,
                            ),
                        )

                def _delete_imagebuilder_component_builds():
                    nonlocal did_work

                    targets_by_name: Dict[str, List[tuple]] = {}
                    for page in _iter_pages(client, "list_components", owner="Self"):
                        for component in page.get("componentVersionList", []) or []:
                            name = component.get("name", "")
                            component_version_arn = component.get("arn", "")
                            if not _is_prefix_name(name) or not component_version_arn:
                                continue
                            for build_page in _iter_pages(
                                client,
                                "list_component_build_versions",
                                componentVersionArn=component_version_arn,
                            ):
                                for build in (
                                    build_page.get("componentSummaryList", []) or []
                                ):
                                    build_arn = build.get("arn", "")
                                    if not build_arn:
                                        continue
                                    version = (
                                        build.get("version")
                                        or component.get("version")
                                        or build_arn.rstrip("/").rsplit("/", 2)[-2]
                                    )
                                    build_number = build_arn.rstrip("/").rsplit("/", 1)[
                                        -1
                                    ]
                                    targets_by_name.setdefault(
                                        f"ImageBuilderComponent {name}", []
                                    ).append(
                                        (f"{version}/{build_number}", build_arn)
                                    )

                    for target_label, items in sorted(targets_by_name.items()):
                        unique_items = sorted(set(items), key=lambda x: x[0])
                        did_work = True
                        if len(unique_items) == 1:
                            version, arn = unique_items[0]
                            _confirm_and_run(
                                f"{target_label} ({version})",
                                lambda a=arn: _ignore_missing(
                                    client.delete_component,
                                    componentBuildVersionArn=a,
                                    ignore_dependency=True,
                                ),
                            )
                            continue

                        def _delete_builds(arns=[arn for _, arn in unique_items]):
                            for a in arns:
                                _ignore_missing(
                                    client.delete_component,
                                    componentBuildVersionArn=a,
                                    ignore_dependency=True,
                                )

                        _confirm_batch_and_run(
                            target_label,
                            [version for version, _ in unique_items],
                            _delete_builds,
                            prompt_label=f"versions of {target_label}",
                        )

                _delete_imagebuilder_resources(
                    "list_image_pipelines",
                    "imagePipelineList",
                    "imagePipelineArn",
                    "delete_image_pipeline",
                    "ImageBuilderPipeline",
                )
                _delete_imagebuilder_resources(
                    "list_image_recipes",
                    "imageRecipeSummaryList",
                    "imageRecipeArn",
                    "delete_image_recipe",
                    "ImageBuilderRecipe",
                    ignore_dependency=True,
                    group_versions=True,
                )
                _delete_imagebuilder_resources(
                    "list_distribution_configurations",
                    "distributionConfigurationSummaryList",
                    "distributionConfigurationArn",
                    "delete_distribution_configuration",
                    "ImageBuilderDistribution",
                )
                _delete_imagebuilder_resources(
                    "list_infrastructure_configurations",
                    "infrastructureConfigurationSummaryList",
                    "infrastructureConfigurationArn",
                    "delete_infrastructure_configuration",
                    "ImageBuilderInfrastructure",
                )
                _delete_imagebuilder_component_builds()

            if ami_wanted:
                _delete_project_amis()

            if _wants(
                "IAMInstanceProfile",
                "IAMInstanceProfiles",
                "InstanceProfile",
                "InstanceProfiles",
            ):
                client = aws_client("iam", region, f"{prefix}-profile-destroy")
                profiles = []
                for page in _iter_pages(client, "list_instance_profiles"):
                    for item in page.get("InstanceProfiles", []) or []:
                        name = item.get("InstanceProfileName", "")
                        if _is_prefix_name(name):
                            profiles.append((name, item.get("Roles", []) or []))
                profile_targets = {
                    (n, tuple(r.get("RoleName", "") for r in rs))
                    for n, rs in profiles
                }
                for name, roles in sorted(profile_targets):
                    if name in protected_role_names:
                        print(f"Keeping protected IAM instance profile: {name}")
                        continue
                    did_work = True

                    def _delete_profile(profile_name=name, role_names=roles):
                        for role_name in role_names:
                            if role_name:
                                _ignore_missing(
                                    client.remove_role_from_instance_profile,
                                    InstanceProfileName=profile_name,
                                    RoleName=role_name,
                                )
                        _ignore_missing(
                            client.delete_instance_profile,
                            InstanceProfileName=profile_name,
                        )

                    _confirm_and_run(f"IAMInstanceProfile {name}", _delete_profile)

            if _wants("RuntimeIAMRole", "RuntimeIAMRoles", "IAMRole", "IAMRoles"):
                client = aws_client("iam", region, f"{prefix}-role-destroy")
                role_names = []
                for page in _iter_pages(client, "list_roles"):
                    for item in page.get("Roles", []) or []:
                        name = item.get("RoleName", "")
                        if _is_prefix_name(name):
                            role_names.append(name)
                for name in sorted(set(role_names)):
                    if name in protected_role_names:
                        print(f"Keeping protected IAM role: {name}")
                        continue
                    did_work = True

                    def _delete_role(role_name=name):
                        for page in _iter_pages(
                            client,
                            "list_attached_role_policies",
                            RoleName=role_name,
                        ):
                            for policy in page.get("AttachedPolicies", []) or []:
                                policy_arn = policy.get("PolicyArn", "")
                                if policy_arn:
                                    _ignore_missing(
                                        client.detach_role_policy,
                                        RoleName=role_name,
                                        PolicyArn=policy_arn,
                                    )
                        for page in _iter_pages(
                            client,
                            "list_role_policies",
                            RoleName=role_name,
                        ):
                            for policy_name in page.get("PolicyNames", []) or []:
                                _ignore_missing(
                                    client.delete_role_policy,
                                    RoleName=role_name,
                                    PolicyName=policy_name,
                                )
                        _ignore_missing(client.delete_role, RoleName=role_name)

                    _confirm_and_run(f"IAMRole {name}", _delete_role)

            if include_all and _wants(
                "SSMParameter",
                "SSMParameters",
                "SecretParameter",
                "SecretParameters",
                "Parameter",
                "Parameters",
            ):
                client = aws_client("ssm", region, f"{prefix}-ssm-destroy")
                names = []
                for startswith in (prefix_dash, f"/{prefix_dash}"):
                    for page in _iter_pages(
                        client,
                        "describe_parameters",
                        ParameterFilters=[
                            {
                                "Key": "Name",
                                "Option": "BeginsWith",
                                "Values": [startswith],
                            }
                        ],
                    ):
                        for item in page.get("Parameters", []) or []:
                            name = item.get("Name", "")
                            if name.startswith(startswith):
                                names.append(name)
                for name in sorted(set(names)):
                    did_work = True
                    _confirm_and_run(
                        f"SSMParameter {name}",
                        lambda n=name: _ignore_missing(client.delete_parameter, Name=n),
                    )

            if include_all and _wants("S3", "Storage", "Storages", "Bucket", "Buckets"):
                client = aws_client("s3", region, f"{prefix}-s3-destroy")
                response = client.list_buckets()
                bucket_names = [
                    b.get("Name", "")
                    for b in response.get("Buckets", []) or []
                    if _is_prefix_name(b.get("Name", ""))
                ]
                for name in sorted(set(bucket_names)):
                    did_work = True

                    def _delete_bucket(bucket_name=name):
                        for page in _iter_pages(
                            client,
                            "list_objects_v2",
                            Bucket=bucket_name,
                        ):
                            objects = [
                                {"Key": obj["Key"]}
                                for obj in page.get("Contents", []) or []
                                if obj.get("Key")
                            ]
                            if objects:
                                _ignore_missing(
                                    client.delete_objects,
                                    Bucket=bucket_name,
                                    Delete={"Objects": objects},
                                )
                        _ignore_missing(client.delete_bucket, Bucket=bucket_name)

                    _confirm_and_run(f"S3Bucket {name}", _delete_bucket)

            if include_all and _wants(
                "DedicatedHost",
                "DedicatedHosts",
                "Host",
                "Hosts",
            ):
                client = aws_client("ec2", region, f"{prefix}-host-destroy")
                host_ids = []
                for page in _iter_pages(client, "describe_hosts"):
                    for host in page.get("Hosts", []) or []:
                        tags = host.get("Tags", []) or []
                        rn = _tag_value(tags, "praktika_rn")
                        name = _tag_value(tags, "Name")
                        if _is_prefix_name(rn) or _is_prefix_name(name):
                            host_id = host.get("HostId", "")
                            if host_id:
                                host_ids.append(host_id)
                for host_id in sorted(set(host_ids)):
                    did_work = True
                    _confirm_and_run(
                        f"DedicatedHost {host_id}",
                        lambda hid=host_id: _ignore_missing(
                            client.release_hosts,
                            HostIds=[hid],
                            ignore_dependency=True,
                        ),
                    )

            if _wants("VPC", "VPCs"):
                from .vpc import VPC

                client = aws_client("ec2", region, f"{prefix}-vpc-discovery")
                names = []
                response = client.describe_vpcs()
                for vpc in response.get("Vpcs", []) or []:
                    name = _resource_name_from_tags(vpc.get("Tags", []) or [])
                    if _is_prefix_name(name):
                        names.append(name)
                for name in sorted(set(names)):
                    did_work = True
                    cfg = VPC.Config(name=name, region=region)
                    _confirm_and_run(
                        f"VPC {name}",
                        lambda c=cfg: _ignore_missing(
                            c.delete,
                            ignore_dependency=True,
                        ),
                    )

            print("\n" + "=" * 60)
            if did_work:
                print(
                    "Destroy completed!"
                    if include_all
                    else "Runtime destroy completed!"
                )
            else:
                print(
                    "No project-prefixed resources found to destroy"
                    if include_all
                    else "No project-prefixed runtime resources found to destroy"
                )
            print("=" * 60)

        def restart_instances(self):
            """Trigger an instance refresh on all configured ASGs."""
            self._verify_account()
            if not self.autoscaling_groups:
                print("No ASGs configured")
                return
            for asg_config in self.autoscaling_groups:
                asg_config.region = self._settings.AWS_REGION
                print("\n" + "=" * 60)
                print(f"Restarting instances in ASG: {asg_config.name}")
                print("=" * 60)
                asg_config.restart()
