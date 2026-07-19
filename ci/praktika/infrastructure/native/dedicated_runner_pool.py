import hashlib
from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.dedicated_host import DedicatedHost
from praktika.infrastructure.ec2_instance import EC2Instance
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile
from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.sqs_queue import SQSQueue

from . import iam_scope
from .runner_pool import (
    _EC2_MESSAGES_STATEMENT,
    _SSM_MANAGED_INSTANCE_CORE_STATEMENT,
    _SSM_MESSAGES_STATEMENT,
    _s3_prefix_resources,
    _secrets_manager_resource,
    _ssm_parameter_resource,
    _unique,
)


@dataclass
class DedicatedRunnerPool:
    """A self-contained CI runner pool backed by AWS Dedicated Hosts and a
    fixed number of EC2 instances (no autoscaling).

    Conceptually this mirrors RunnerPool: one SQSQueue whose name matches the
    `runs_on` label 1:1 (so praktika routes jobs without extra configuration),
    plus the compute that serves it and a scoped IAM role/instance profile.
    The difference is the compute model:

    - RunnerPool = LaunchTemplate + AutoScalingGroup, elastic capacity that
      scales between 0 and `max_size` on queue demand.
    - DedicatedRunnerPool = a DedicatedHost pool (scarce, physically dedicated
      capacity such as Mac `*.metal` hosts) plus a fixed `quantity_per_az`
      EC2 instances per availability zone launched with tenancy="host". There
      is no scale-in/scale-out — the fleet size is constant.

    Use it for runner types that cannot live on shared tenancy or cannot be
    provisioned on demand (macOS being the canonical case: Apple licensing
    requires dedicated hosts and a minimum 24h host allocation, so autoscaling
    them makes no sense).

    Like RunnerPool, runtime SSM Parameter Store, Secrets Manager, and S3
    access are opt-in through `allowed_ssm_parameters`, `allowed_secrets`,
    `allowed_s3_prefixes` (read+write), and `allowed_s3_prefixes_readonly`
    (read-only); each has an `allow_all_*` escape hatch. SSM debugging is
    opt-in through `allow_ssm_debug`. Bare names are project namespaced by
    CloudInfrastructure.Config; full ARNs are preserved as-is.

    All child components — the SQSQueue, DedicatedHost pool, one EC2Instance
    group per availability zone, and (unless `iam_instance_profile_name`
    points at a pre-existing external profile) an IAMRole + IAMInstanceProfile
    — are created at construction time and registered into
    CloudInfrastructure.Config automatically via its dedicated_runner_pools
    list.

    Example::

        pool = DedicatedRunnerPool(
            name="pr-macos-m2",
            instance_type="mac2-m2pro.metal",
            availability_zones=["ap-southeast-2b"],
            quantity_per_az=6,
            image_id="ami-...",
            subnet_id="subnet-...",
            security_group_ids=["sg-..."],
            user_data_file="./ci/infra/scripts/user_data_macos.txt",
        )
    """

    name: str
    instance_type: str
    availability_zones: List[str]
    quantity_per_az: int
    image_id: str = ""
    region: str = ""
    # Networking mirrors RunnerPool: reference the VPC by name and let the
    # subnet (per AZ) and security groups (by name, defaulting to
    # "{vpc_name}-sg") resolve at deploy time. Raw `subnet_id` /
    # `security_group_ids` / `subnet_ids_by_az` can still be given to override
    # the lookup (e.g. for a VPC without the Praktika naming convention).
    vpc_name: str = ""
    security_group_names: List[str] = field(default_factory=list)
    subnet_id: str = ""
    subnet_ids_by_az: Dict[str, str] = field(default_factory=dict)
    security_group_ids: List[str] = field(default_factory=list)
    key_name: str = ""
    user_data: str = ""
    user_data_file: str = ""
    root_volume_size_gb: int = 100
    root_volume_type: str = "gp3"
    root_volume_encrypted: bool = True
    # Optional tag stamped onto the Dedicated Hosts for bookkeeping. Host/
    # instance auto-placement matches by instance type + AZ, so this is not
    # required for placement; the pool name (praktika_rn tag) already
    # identifies the hosts.
    praktika_resource_tag: str = ""
    # github:runner-type tag value; defaults to `name`. Accepts a list of
    # labels (joined with "," into the single tag), which the legacy GitHub
    # runner-init expands into multiple runner labels.
    runner_type: "str | List[str]" = ""

    # IAM: by default a scoped role + instance profile are generated from the
    # allow_* fields below (RunnerPool-style). Set `iam_instance_profile_name`
    # to reuse a pre-existing external profile instead; in that case no role or
    # profile is generated and the allow_* fields are ignored.
    iam_instance_profile_name: str = ""
    ec2_role: IAMRole.Config | None = None
    instance_profile: IAMInstanceProfile.Config | None = None
    allowed_ssm_parameters: List[str] = field(default_factory=list)
    allowed_secrets: List[str] = field(default_factory=list)
    allowed_s3_prefixes: List[str] = field(default_factory=list)
    allowed_s3_prefixes_readonly: List[str] = field(default_factory=list)
    allow_all_ssm_parameters: bool = False
    allow_all_secrets: bool = False
    allow_all_s3_prefixes: bool = False
    allow_ssm_debug: bool = False
    # Extra raw IAM policy statements appended verbatim to the generated
    # instance role (ignored when an external iam_instance_profile_name is
    # used). Use for permissions the allow_* lists can't express, e.g.
    # ec2:DescribeInstances that the legacy GitHub runner-init needs to read
    # its own instance tags.
    iam_statements: List[Dict[str, Any]] = field(default_factory=list)

    tags: Dict[str, str] = field(default_factory=dict)
    ext: Dict[str, Any] = field(default_factory=dict)

    queue: SQSQueue.Config = field(init=False)
    dedicated_hosts: List[DedicatedHost.Config] = field(init=False)
    ec2_instances: List[EC2Instance.Config] = field(init=False)

    def _resolved_region(self) -> str:
        if self.region:
            return self.region
        if self.availability_zones:
            # AZ name is region + one letter suffix: "ap-southeast-2b" -> "ap-southeast-2".
            return self.availability_zones[0][:-1]
        raise ValueError(
            f"Cannot determine region for DedicatedRunnerPool '{self.name}': "
            f"set 'region' or 'availability_zones'"
        )

    def _build_runner_iam(self, queue_name: str):
        # allow_all_* grants access to every resource in the project's
        # namespace (the "{slug}-"/"{slug}/" name prefix), not the whole
        # account. To reach resources outside the namespace, list their exact
        # ARNs in the allowed_* fields instead.
        allowed_ssm_parameter_resources = (
            iam_scope.ssm_parameter_arns()
            if self.allow_all_ssm_parameters
            else [
                _ssm_parameter_resource(name)
                for name in self.allowed_ssm_parameters
                if name and name.strip()
            ]
        )
        allowed_secret_resources = (
            iam_scope.secret_arns()
            if self.allow_all_secrets
            else [
                _secrets_manager_resource(name)
                for name in self.allowed_secrets
                if name and name.strip()
            ]
        )
        allowed_s3_resources = (
            iam_scope.project_bucket_arns()
            if self.allow_all_s3_prefixes
            else _unique(
                [
                    resource
                    for prefix in self.allowed_s3_prefixes
                    if prefix and prefix.strip()
                    for resource in _s3_prefix_resources(prefix)
                ]
            )
        )
        readonly_s3_resources = _unique(
            [
                resource
                for prefix in self.allowed_s3_prefixes_readonly
                if prefix and prefix.strip()
                for resource in _s3_prefix_resources(prefix)
            ]
        )
        # Unlike RunnerPool there is no AutoScalingSelfTerminate statement:
        # capacity is fixed, instances are never self-terminated via an ASG.
        runner_statements = [
            {
                "Sid": "SQSReceiveDelete",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:SendMessage",
                    "sqs:GetQueueUrl",
                    "sqs:GetQueueAttributes",
                ],
                "Resource": f"arn:aws:sqs:*:*:{queue_name}",
            },
        ]
        if self.allow_ssm_debug:
            runner_statements = [
                _SSM_MANAGED_INSTANCE_CORE_STATEMENT,
                _SSM_MESSAGES_STATEMENT,
                _EC2_MESSAGES_STATEMENT,
            ] + runner_statements
        if allowed_s3_resources:
            runner_statements.append(
                {
                    "Sid": "AllowedS3ReadWrite",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectTagging",
                        "s3:HeadObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:PutObject",
                        "s3:PutObjectTagging",
                        "s3:AbortMultipartUpload",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                    ],
                    "Resource": allowed_s3_resources,
                }
            )
        if readonly_s3_resources:
            runner_statements.append(
                {
                    "Sid": "AllowedS3ReadOnly",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectTagging",
                        "s3:HeadObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": readonly_s3_resources,
                }
            )
        if allowed_ssm_parameter_resources:
            runner_statements.append(
                {
                    "Sid": "AllowedSSMParametersRead",
                    "Effect": "Allow",
                    "Action": [
                        "ssm:GetParameter",
                        "ssm:GetParameters",
                    ],
                    "Resource": allowed_ssm_parameter_resources,
                }
            )
        if allowed_secret_resources:
            runner_statements.append(
                {
                    "Sid": "AllowedSecretsManagerSecretsRead",
                    "Effect": "Allow",
                    "Action": [
                        "secretsmanager:DescribeSecret",
                        "secretsmanager:GetSecretValue",
                    ],
                    "Resource": allowed_secret_resources,
                }
            )
        # Caller-supplied extra statements (e.g. ec2:DescribeInstances).
        runner_statements.extend(self.iam_statements)
        self.ec2_role = IAMRole.Config(
            name=f"{self.name}-role",
            trust_service="ec2.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
            ],
            inline_policies={
                "RunnerAccess": {
                    "Version": "2012-10-17",
                    "Statement": runner_statements,
                }
            },
        )
        self.instance_profile = IAMInstanceProfile.Config(
            name=f"{self.name}-profile",
            role_name=self.ec2_role.name,
        )

    def __post_init__(self):
        assert self.availability_zones, (
            f"DedicatedRunnerPool '{self.name}' requires at least one "
            f"availability zone"
        )
        assert (
            self.quantity_per_az >= 1
        ), f"quantity_per_az={self.quantity_per_az} must be >= 1"
        assert self.vpc_name or self.subnet_id or self.subnet_ids_by_az, (
            f"DedicatedRunnerPool '{self.name}' needs 'vpc_name' (recommended) "
            f"or a raw 'subnet_id'/'subnet_ids_by_az' to place instances"
        )

        queue_name = self.name
        region = self._resolved_region()
        if isinstance(self.runner_type, (list, tuple)):
            runner_type = ",".join(t for t in self.runner_type if t) or self.name
        else:
            runner_type = self.runner_type or self.name

        # Default the security groups to the VPC's "{vpc_name}-sg" convention,
        # matching RunnerPool, unless raw IDs or explicit names were given.
        security_group_names = list(self.security_group_names)
        if not security_group_names and not self.security_group_ids and self.vpc_name:
            security_group_names = [f"{self.vpc_name}-sg"]

        # Generate a scoped IAM role/profile unless an external profile is
        # explicitly provided.
        if self.iam_instance_profile_name:
            self.ec2_role = None
            self.instance_profile = None
            instance_profile_name = self.iam_instance_profile_name
        else:
            self._build_runner_iam(queue_name)
            instance_profile_name = self.instance_profile.name

        self.queue = SQSQueue.Config(
            name=queue_name,
            visibility_timeout=1800,
            message_retention=86400,
        )

        # Tags that let the praktika controller route jobs from `queue_name` to
        # these instances, mirroring RunnerPool's runtime tags 1:1. These are
        # pool-scoped (shared by every unit) — routing is by pool, not by the
        # per-unit Name below.
        runtime_tags = {
            "praktika_pool": self.name,
            "praktika_role": "job_runner",
            "praktika_queue": queue_name,
        }
        runtime_tags.update(self.tags or {})

        # Fixed fleet, one "unit" (one Dedicated Host + one EC2 instance) at a
        # time so each resource gets its own identifiable Name. A host and its
        # paired instance share a hex id ("{name}-{hex}"). The id is a stable
        # deterministic hash of pool+AZ+index, so it is preserved across
        # deploys — re-deploying matches existing hosts/instances by name and
        # keeps them instead of creating duplicates.
        self.dedicated_hosts = []
        self.ec2_instances = []
        for az in self.availability_zones:
            # A raw subnet (per-AZ or shared) overrides the vpc_name lookup;
            # otherwise EC2Instance resolves the subnet from `vpc_name` by AZ.
            subnet_id = self.subnet_ids_by_az.get(az, self.subnet_id)
            for index in range(self.quantity_per_az):
                unit_id = hashlib.sha1(
                    f"{self.name}-{az}-{index}".encode()
                ).hexdigest()[:6]
                unit_name = f"{self.name}-{unit_id}"
                self.dedicated_hosts.append(
                    DedicatedHost.Config(
                        name=unit_name,
                        region=self.region,
                        availability_zones=[az],
                        instance_type=self.instance_type,
                        auto_placement="on",
                        quantity_per_az=1,
                        praktika_resource_tag=self.praktika_resource_tag,
                    )
                )
                self.ec2_instances.append(
                    EC2Instance.Config(
                        name=unit_name,
                        region=region,
                        quantity=1,
                        image_id=self.image_id,
                        instance_type=self.instance_type,
                        vpc_name=self.vpc_name,
                        availability_zone=az,
                        subnet_id=subnet_id,
                        security_group_ids=list(self.security_group_ids),
                        security_group_names=list(security_group_names),
                        iam_instance_profile_name=instance_profile_name,
                        key_name=self.key_name,
                        user_data=self.user_data,
                        user_data_file=self.user_data_file,
                        root_volume_type=self.root_volume_type,
                        root_volume_size=self.root_volume_size_gb,
                        root_volume_encrypted=self.root_volume_encrypted,
                        tenancy="host",
                        praktika_resource_tag="runner",
                        runner_type=runner_type,
                        tags=dict(runtime_tags),
                    )
                )
