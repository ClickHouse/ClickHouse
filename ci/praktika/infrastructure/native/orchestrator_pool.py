import copy
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.image_builder import ImageBuilder
from praktika.infrastructure.autoscaling_group import AutoScalingGroup
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile
from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.lambda_function import Lambda
from praktika.infrastructure.launch_template import LaunchTemplate
from praktika.infrastructure.secret_parameter import SecretParameter
from praktika.infrastructure.sqs_queue import SQSQueue
from praktika.settings import Settings

from . import iam_scope

from .configs import (
    ORCHESTRATOR_INSTANCE_PROFILE_NAME,
    ORCHESTRATOR_ROLE_NAME,
    lambda_gh_trigger_config,
)

GH_TRIGGER_ROLE_NAME = "gh-webhook-role"
GH_TRIGGER_WEBHOOK_SECRET_NAME = "gh-webhook-secret"

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


@dataclass
class OrchestratorPool:
    """A self-contained CI workflow orchestrator pool: one LaunchTemplate
    and one AutoScalingGroup that run the praktika orchestrator process.

    The orchestrator polls the workflow-trigger SQS queue and dispatches
    jobs to per-runner-type queues. min_size is always 0; `size` sets the
    desired capacity and `max_size` caps the pool. When auto-scaled,
    `capacity_reserve` keeps that many extra idle instances above the queue
    demand.

    The pool assumes the selected AMI already contains the Praktika workflow
    runtime and systemd unit. By default it enables `praktika-controller` at
    boot; `user_data` can override that when extra instance boot customization
    is required.

    `ext["allowed_push_branches"]` controls which GitHub push branch refs the
    webhook Lambda accepts for this pool. The default is
    `[Settings.MAIN_BRANCH]` (the project's main branch).
    `ext["allowed_users"]` optionally restricts pull_request webhook events to
    a fixed set of GitHub logins.
    `ext["allowed_repositories"]` optionally restricts the webhook Lambda to a
    fixed set of repository full names such as `["ClickHouse/praktika"]`.
    `ext["external_pr_autoapprove_paths"]` optionally lists glob patterns that
    may autoapprove a new external-PR head after a previously approved head,
    but only when the new delta touches files entirely within those patterns.

    Registered into CloudInfrastructure.Config automatically via its
    orchestrator_pool field.

    Example::

        pool = OrchestratorPool(
            ami_id="ami-...",
            security_group_ids=["sg-..."],
            vpc_name="ci-cd",
            iam_instance_profile_name="workflow-orchestrator-profile",
            instance_type="t4g.small",
            size=2,
            max_size=2,
        )
    """

    class Scaling:
        Disabled = "disabled"
        Auto = "auto"

    instance_type: str
    size: int
    max_size: int
    vpc_name: str = ""
    name: str = "workflow-orchestrator"
    scaling: str = Scaling.Disabled
    ami_id: str = ""  # resolved at deploy time via SSM if empty
    image_builder: ImageBuilder.Config | None = None
    user_data: str = ""
    iam_instance_profile_name: str = ORCHESTRATOR_INSTANCE_PROFILE_NAME
    ec2_role_name: str = ORCHESTRATOR_ROLE_NAME
    security_group_ids: List[str] = field(default_factory=list)
    security_group_names: List[str] = field(default_factory=list)
    volume_size_gb: int = 30
    capacity_reserve: int = 0
    # TODO: Consider updating ext["allowed_push_branches"] automatically from MainCI push workflow branches.
    ext: Dict[str, Any] = field(default_factory=dict)

    launch_template: LaunchTemplate.Config = field(init=False)
    autoscaling_group: AutoScalingGroup.Config = field(init=False)
    ec2_role: IAMRole.Config = field(init=False)
    instance_profile: IAMInstanceProfile.Config = field(init=False)
    lambda_config: Lambda.Config = field(init=False)
    lambda_role: IAMRole.Config = field(init=False)
    webhook_secret: SecretParameter.Config = field(init=False)
    queue: SQSQueue.Config = field(init=False)

    def _queue_name(self) -> str:
        return self.name

    def _asg_name(self) -> str:
        return self.name

    def _launch_template_name(self) -> str:
        return f"{self.name}-lt"

    def _lambda_name(self) -> str:
        return self.name

    def _lambda_role_name(self) -> str:
        return GH_TRIGGER_ROLE_NAME

    def _webhook_secret_name(self) -> str:
        return GH_TRIGGER_WEBHOOK_SECRET_NAME

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
            f"OrchestratorPool scaling={self.scaling!r} is not supported; "
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
        allowed_push_branches = self.ext.get(
            "allowed_push_branches", [Settings.MAIN_BRANCH]
        )
        assert isinstance(
            allowed_push_branches, list
        ), "ext['allowed_push_branches'] must be a list of branch names"
        assert all(
            isinstance(branch, str) and branch.strip()
            for branch in allowed_push_branches
        ), "ext['allowed_push_branches'] must contain only non-empty strings"
        allowed_push_branches = [branch.strip() for branch in allowed_push_branches]
        allowed_repositories = self.ext.get("allowed_repositories", [])
        assert isinstance(
            allowed_repositories, list
        ), "ext['allowed_repositories'] must be a list of repository full names"
        assert all(
            isinstance(repo, str) and repo.strip()
            for repo in allowed_repositories
        ), "ext['allowed_repositories'] must contain only non-empty strings"
        allowed_repositories = [repo.strip() for repo in allowed_repositories]
        allowed_users = self.ext.get("allowed_users", [])
        assert isinstance(
            allowed_users, list
        ), "ext['allowed_users'] must be a list of GitHub logins"
        assert all(
            isinstance(user, str) and user.strip()
            for user in allowed_users
        ), "ext['allowed_users'] must contain only non-empty strings"
        allowed_users = [user.strip() for user in allowed_users]
        external_pr_autoapprove_paths = self.ext.get(
            "external_pr_autoapprove_paths", []
        )
        assert isinstance(
            external_pr_autoapprove_paths, list
        ), "ext['external_pr_autoapprove_paths'] must be a list of glob patterns"
        assert all(
            isinstance(pattern, str) and pattern.strip()
            for pattern in external_pr_autoapprove_paths
        ), "ext['external_pr_autoapprove_paths'] must contain only non-empty strings"
        external_pr_autoapprove_paths = [
            pattern.strip() for pattern in external_pr_autoapprove_paths
        ]
        # Extra IAM policy statements appended to WorkflowOrchestratorAccess, so a
        # project can grant pool-specific permissions (e.g. the AI advisor's
        # Bedrock access) from its config without editing this shared class.
        extra_iam_statements = self.ext.get("iam_statements", [])
        assert isinstance(extra_iam_statements, list) and all(
            isinstance(stmt, dict) for stmt in extra_iam_statements
        ), "ext['iam_statements'] must be a list of IAM policy statement dicts"
        assert (
            self.max_size >= self.capacity_reserve
        ), f"max_size={self.max_size} must be >= capacity_reserve={self.capacity_reserve}"
        queue_name = self._queue_name()
        asg_name = self._asg_name()

        artifact_bucket = (Settings.S3_ARTIFACT_BUCKET or "").strip()
        artifact_resources = (
            [
                f"arn:aws:s3:::{artifact_bucket}/runs/*/cancel-request",
                f"arn:aws:s3:::{artifact_bucket}/pr/*/cancel-before*",
                f"arn:aws:s3:::{artifact_bucket}/external-pr-approvals/*",
            ]
            if artifact_bucket
            else [
                "arn:aws:s3:::*/runs/*/cancel-request",
                "arn:aws:s3:::*/pr/*/cancel-before*",
                "arn:aws:s3:::*/external-pr-approvals/*",
            ]
        )

        self.ec2_role = IAMRole.Config(
            name=self.ec2_role_name,
            trust_service="ec2.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
                "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
            ],
            inline_policies={
                "WorkflowOrchestratorAccess": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            # Orchestrator receives from its own queue and
                            # dispatches job tasks to the project's runner
                            # queues; all share the project-slug prefix.
                            # Queues are created at deploy time, so no
                            # Create/DeleteQueue at runtime.
                            "Sid": "SQSReadDeleteSend",
                            "Effect": "Allow",
                            "Action": [
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:ChangeMessageVisibility",
                                "sqs:SendMessage",
                                "sqs:GetQueueUrl",
                                "sqs:GetQueueAttributes",
                            ],
                            "Resource": iam_scope.sqs_queue_arns(),
                        },
                        {
                            "Sid": "S3ReadWrite",
                            "Effect": "Allow",
                            "Action": [
                                "s3:GetObject",
                                "s3:HeadObject",
                                "s3:ListBucket",
                                "s3:GetBucketLocation",
                                "s3:PutObject",
                                "s3:AbortMultipartUpload",
                            ],
                            "Resource": iam_scope.project_bucket_arns(),
                        },
                        {
                            # Scale-out is performed by the autoscaler Lambda
                            # (UpdateAutoScalingGroup) and the ASG service, not
                            # this role; scale-in is self-termination via the
                            # ASG. No ec2:RunInstances / ec2:TerminateInstances.
                            "Sid": "AutoScalingSelfTerminate",
                            "Effect": "Allow",
                            "Action": [
                                "autoscaling:TerminateInstanceInAutoScalingGroup",
                            ],
                            "Resource": iam_scope.autoscaling_group_arns(),
                        },
                        {
                            "Sid": "CloudWatchLogs",
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            "Resource": iam_scope.cloudwatch_log_group_arns(),
                        },
                        *extra_iam_statements,
                    ],
                }
            },
        )
        self.instance_profile = IAMInstanceProfile.Config(
            name=self.iam_instance_profile_name,
            role_name=self.ec2_role_name,
        )
        runtime_tags = {
            "praktika_pool": self.name,
            "praktika_role": "workflow_orchestrator",
            "praktika_queue": queue_name,
            "praktika_asg": asg_name,
            "praktika_scaling": self.scaling,
            "praktika_capacity_reserve": str(self.capacity_reserve),
        }
        self.launch_template = LaunchTemplate.Config(
            name=self._launch_template_name(),
            image_id=self.ami_id,
            image_builder=self.image_builder,
            instance_type=self.instance_type,
            security_group_ids=self.security_group_ids,
            security_group_names=self.security_group_names,
            vpc_name=self.vpc_name,
            iam_instance_profile_name=self.iam_instance_profile_name,
            set_default_version_to_latest=True,
            user_data=self.user_data,
            root_volume_size_gb=self.volume_size_gb,
            root_volume_type="gp3",
            tags=runtime_tags,
            praktika_resource_tag="workflow_orchestrator",
        )
        if self.image_builder:
            self.image_builder.launch_templates.append(self.launch_template)
        self.lambda_config = copy.deepcopy(lambda_gh_trigger_config)
        self.lambda_config.name = self._lambda_name()
        self.lambda_config.role_name = self._lambda_role_name()
        self.lambda_config.secrets = {
            self._webhook_secret_name(): "GH_WEBHOOK_SECRET",
        }
        self.lambda_config.environments["SQS_QUEUE_NAME"] = queue_name
        self.lambda_config.environments["ALLOWED_PUSH_BRANCHES"] = ",".join(
            allowed_push_branches
        )
        self.lambda_config.environments["ALLOWED_REPOSITORIES_JSON"] = json.dumps(
            allowed_repositories,
            sort_keys=True,
        )
        self.lambda_config.environments["ALLOWED_USERS_JSON"] = json.dumps(
            allowed_users,
            sort_keys=True,
        )
        self.lambda_config.environments["GH_AUTH_LAMBDA_NAME"] = (
            Settings.GH_AUTH_LAMBDA_NAME or ""
        )
        self.lambda_config.environments["EXTERNAL_PR_AUTOAPPROVE_PATHS_JSON"] = (
            json.dumps(
                external_pr_autoapprove_paths,
                sort_keys=True,
            )
        )
        self.webhook_secret = SecretParameter.Config(
            name=self._webhook_secret_name(),
            description="GitHub webhook secret for the workflow trigger Lambda",
            generate_random=True,
        )
        self.lambda_role = IAMRole.Config(
            name=self._lambda_role_name(),
            trust_service="lambda.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
            ],
            inline_policies={
                # Lambda enqueues workflow trigger events to the main
                # workflow queue and writes cancel signals to S3 (per-run
                # cancel-request and per-PR scoped cancel-before).
                "SQSSendMessage": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["sqs:SendMessage", "sqs:GetQueueUrl"],
                            "Resource": iam_scope.sqs_queue_arns(),
                        },
                    ],
                },
                "S3CancelSignal": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": [f"arn:aws:s3:::{artifact_bucket}"],
                        },
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:PutObject"],
                            "Resource": [
                                *artifact_resources,
                            ],
                        },
                    ],
                },
            },
        )
        self.queue = SQSQueue.Config(
            name=queue_name,
            visibility_timeout=600,
            message_retention=86400,
        )
        self.autoscaling_group = AutoScalingGroup.Config(
            name=asg_name,
            vpc_name=self.vpc_name,
            availability_zones=[],
            min_size=0,
            max_size=self.max_size,
            desired_capacity=self.size,
            launch_template_name=self._launch_template_name(),
            launch_template_version="$Default" if self.image_builder else "$Latest",
            tags=runtime_tags,
            praktika_resource_tag="workflow_orchestrator",
        )
