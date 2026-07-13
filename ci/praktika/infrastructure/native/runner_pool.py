from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.image_builder import ImageBuilder
from praktika.infrastructure.autoscaling_group import AutoScalingGroup
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile
from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.launch_template import LaunchTemplate
from praktika.infrastructure.sqs_queue import SQSQueue

from . import iam_scope

# Lets the local SSM Agent register and report this EC2 instance as a managed
# instance. This intentionally omits Parameter Store read actions from AWS's
# AmazonSSMManagedInstanceCore managed policy; runner access to secrets/params
# must come through allowed_ssm_parameters or allow_all_ssm_parameters.
#
# Security note: job code using the instance role can still call these APIs.
# They do not grant Run Command, Session Manager, or parameter reads, but they
# do allow writing SSM inventory/compliance/association status metadata and
# reading SSM documents. For public/untrusted jobs, prefer a dedicated runner
# pool with the minimum required role surface and no sensitive SSM documents.
_SSM_MANAGED_INSTANCE_CORE_STATEMENT = {
    "Sid": "SSMManagedInstanceCore",
    "Effect": "Allow",
    "Action": [
        "ssm:DescribeAssociation",
        "ssm:GetDeployablePatchSnapshotForInstance",
        "ssm:GetDocument",
        "ssm:DescribeDocument",
        "ssm:GetManifest",
        "ssm:ListAssociations",
        "ssm:ListInstanceAssociations",
        "ssm:PutInventory",
        "ssm:PutComplianceItems",
        "ssm:PutConfigurePackageResult",
        "ssm:UpdateAssociationStatus",
        "ssm:UpdateInstanceAssociationStatus",
        "ssm:UpdateInstanceInformation",
    ],
    "Resource": "*",
}

# Lets SSM Agent maintain Session Manager / Run Command control and data
# channels after another principal with the right permissions starts an SSM
# operation. This role still cannot start sessions or send commands by itself.
#
# Security note: untrusted code with instance-role credentials could attempt to
# interfere with the local agent channels, so this is operationally useful but
# not a hard isolation boundary.
_SSM_MESSAGES_STATEMENT = {
    "Sid": "SSMMessages",
    "Effect": "Allow",
    "Action": [
        "ssmmessages:CreateControlChannel",
        "ssmmessages:CreateDataChannel",
        "ssmmessages:OpenControlChannel",
        "ssmmessages:OpenDataChannel",
    ],
    "Resource": "*",
}

# Legacy SSM Agent message delivery APIs used to poll and acknowledge work for
# this managed instance. These are needed for compatibility with SSM Agent
# control-plane behavior on EC2.
#
# Security note: this does not expose Parameter Store or Secrets Manager, but
# untrusted code with instance-role credentials could still call these instance
# messaging APIs. Avoid attaching this statement to OSS pools if SSM management
# of those instances is not required.
_EC2_MESSAGES_STATEMENT = {
    "Sid": "EC2Messages",
    "Effect": "Allow",
    "Action": [
        "ec2messages:AcknowledgeMessage",
        "ec2messages:DeleteMessage",
        "ec2messages:FailMessage",
        "ec2messages:GetEndpoint",
        "ec2messages:GetMessages",
        "ec2messages:SendReply",
    ],
    "Resource": "*",
}

_DEFAULT_PRAKTIKA_CONTROLLER_USER_DATA = "\n".join(
    [
        "#!/usr/bin/env bash",
        "set -xeuo pipefail",
        "",
        "# Add any host customization you need above this line.",
        "/usr/local/bin/praktika-configure-cloudwatch-agent",
        "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/etc/praktika/amazon-cloudwatch-agent.json -s",
        "systemctl enable --now praktika-controller",
        "",
    ]
)


def _ssm_parameter_resource(name_or_arn: str) -> str:
    value = name_or_arn.strip()
    if value.startswith("arn:"):
        return value
    return f"arn:aws:ssm:*:*:parameter/{value.lstrip('/')}"


def _secrets_manager_resource(name_or_arn: str) -> str:
    value = name_or_arn.strip()
    if value.startswith("arn:"):
        return value
    return f"arn:aws:secretsmanager:*:*:secret:{value}*"


def _s3_prefix_resources(prefix_or_arn: str) -> List[str]:
    value = prefix_or_arn.strip()
    if value.startswith("arn:"):
        return [value]
    value = value.removeprefix("s3://").lstrip("/")
    if not value:
        return []
    bucket, _, prefix = value.partition("/")
    if not bucket:
        return []
    if not prefix:
        return [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
    prefix = prefix.strip("/")
    return [
        f"arn:aws:s3:::{bucket}",
        f"arn:aws:s3:::{bucket}/{prefix}",
        f"arn:aws:s3:::{bucket}/{prefix}/*",
    ]


def _unique(values: List[str]) -> List[str]:
    result = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


@dataclass
class RunnerPool:
    """A self-contained CI runner pool: one LaunchTemplate, AutoScalingGroup,
    and SQSQueue that together form a single runner type.

    The ASG always starts with min_size=0; `size` sets the desired capacity
    and `max_size` caps the pool. When auto-scaled, `capacity_reserve` keeps
    that many extra idle instances above the queue demand. The queue name
    matches the `runs_on` label 1:1 so praktika routes jobs without extra
    configuration.

    The pool assumes the selected AMI already contains the Praktika runner
    runtime and systemd unit. By default it enables `praktika-controller` at
    boot; `user_data` can override that when extra instance boot customization
    is required.

    Runtime SSM Parameter Store, Secrets Manager, and S3 access are opt-in
    through `allowed_ssm_parameters`, `allowed_secrets`, and
    `allowed_s3_prefixes` (read+write); `allowed_s3_prefixes_readonly` grants
    read-only S3 access (GetObject/ListBucket, no writes). Bare names are
    project namespaced by CloudInfrastructure.Config; full ARNs are preserved
    as-is. Each resource type also has an explicit `allow_all_*` escape hatch.

    SSM debugging is opt-in through `allow_ssm_debug`. When enabled, the
    runner instance role gets only the instance-side SSM Agent permissions
    needed to register, receive commands/sessions, and reply. It still cannot
    issue SSM commands or start sessions against any runner.

    All three AWS components are created at construction time and registered
    into CloudInfrastructure.Config automatically via its runner_pools list.

    Example::

        pool = RunnerPool(
            name="arm-small",
            instance_type="m8g.2xlarge",
            ami_id="ami-...",
            security_group_ids=["sg-..."],
            vpc_name="ci-cd",
            scaling=RunnerPool.Scaling.Disabled,
            size=1,
            max_size=2,
            volume_size_gb=100,
        )
    """

    class Scaling:
        Disabled = "disabled"
        Auto = "auto"

    name: str
    instance_type: str
    scaling: str
    size: int
    max_size: int
    vpc_name: str = ""
    ami_id: str = ""  # resolved at deploy time via SSM if empty
    image_builder: ImageBuilder.Config | None = None
    user_data: str = ""
    ec2_role: IAMRole.Config | None = None
    instance_profile: IAMInstanceProfile.Config | None = None
    allowed_ssm_parameters: List[str] = field(default_factory=list)
    allowed_secrets: List[str] = field(default_factory=list)
    allowed_s3_prefixes: List[str] = field(default_factory=list)
    # Read-only S3 prefixes: GetObject/ListBucket only, no writes. Same
    # bare-name/ARN handling as `allowed_s3_prefixes`.
    allowed_s3_prefixes_readonly: List[str] = field(default_factory=list)
    allow_all_ssm_parameters: bool = False
    allow_all_secrets: bool = False
    allow_all_s3_prefixes: bool = False
    allow_ssm_debug: bool = False
    security_group_ids: List[str] = field(default_factory=list)
    security_group_names: List[str] = field(default_factory=list)
    volume_size_gb: int = 30
    capacity_reserve: int = 0
    ext: Dict[str, Any] = field(default_factory=dict)

    launch_template: LaunchTemplate.Config = field(init=False)
    autoscaling_group: AutoScalingGroup.Config = field(init=False)
    queue: SQSQueue.Config = field(init=False)

    def __post_init__(self):
        if not self.user_data:
            self.user_data = _DEFAULT_PRAKTIKA_CONTROLLER_USER_DATA
        if (
            self.vpc_name
            and not self.security_group_ids
            and not self.security_group_names
        ):
            self.security_group_names = [f"{self.vpc_name}-sg"]
        assert self.scaling in (self.Scaling.Disabled, self.Scaling.Auto), (
            f"RunnerPool scaling={self.scaling!r} is not supported; "
            f"use Scaling.Disabled or Scaling.Auto"
        )
        min_size = 0 if self.scaling == self.Scaling.Auto else 1
        assert self.size >= min_size, (
            f"size={self.size} is invalid for scaling={self.scaling!r}; "
            f"must be >= {min_size}"
        )
        assert (
            self.max_size >= self.size
        ), f"max_size={self.max_size} must be >= size={self.size}"
        assert (
            self.capacity_reserve >= 0
        ), f"capacity_reserve={self.capacity_reserve} must be >= 0"
        assert (
            self.max_size >= self.capacity_reserve
        ), f"max_size={self.max_size} must be >= capacity_reserve={self.capacity_reserve}"

        queue_name = self.name
        asg_name = self.name
        launch_template_name = f"{self.name}-lt"

        if self.ec2_role is None:
            # allow_all_* grants access to every resource in the project's
            # namespace (the "{slug}-"/"{slug}/" name prefix), not the whole
            # account. To reach resources outside the namespace, list their
            # exact ARNs in the allowed_* fields instead.
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
                {
                    # Scale-in is self-termination via the ASG (performed by
                    # the controller on the instance). No ec2:TerminateInstances
                    # — direct instance termination is not needed.
                    "Sid": "AutoScalingSelfTerminate",
                    "Effect": "Allow",
                    "Action": [
                        "autoscaling:TerminateInstanceInAutoScalingGroup",
                    ],
                    "Resource": iam_scope.autoscaling_group_arns(),
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
        if self.instance_profile is None:
            self.instance_profile = IAMInstanceProfile.Config(
                name=f"{self.name}-profile",
                role_name=self.ec2_role.name,
            )
        runtime_tags = {
            "praktika_pool": self.name,
            "praktika_role": "job_runner",
            "praktika_queue": queue_name,
            "praktika_asg": asg_name,
            "praktika_scaling": self.scaling,
            "praktika_capacity_reserve": str(self.capacity_reserve),
        }
        self.launch_template = LaunchTemplate.Config(
            name=launch_template_name,
            image_id=self.ami_id,
            image_builder=self.image_builder,
            instance_type=self.instance_type,
            security_group_ids=self.security_group_ids,
            security_group_names=self.security_group_names,
            vpc_name=self.vpc_name,
            iam_instance_profile_name=self.instance_profile.name,
            set_default_version_to_latest=True,
            user_data=self.user_data,
            root_volume_size_gb=self.volume_size_gb,
            root_volume_type="gp3",
            tags=runtime_tags,
            praktika_resource_tag="runner",
        )
        if self.image_builder:
            self.image_builder.launch_templates.append(self.launch_template)
        self.autoscaling_group = AutoScalingGroup.Config(
            name=asg_name,
            vpc_name=self.vpc_name,
            availability_zones=[],
            min_size=0,
            max_size=self.max_size,
            desired_capacity=self.size,
            launch_template_name=launch_template_name,
            launch_template_version="$Default" if self.image_builder else "$Latest",
            tags=runtime_tags,
            praktika_resource_tag="runner",
        )
        self.queue = SQSQueue.Config(
            name=queue_name,
            visibility_timeout=1800,
            message_retention=86400,
        )
