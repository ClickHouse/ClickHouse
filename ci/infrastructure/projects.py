from ci.settings.settings import PROJECT_NAME, PRAKTIKA_BASE_VENV
from praktika.infrastructure import Components, ImageBuilder, VPC
from praktika.infrastructure.cloud import CloudInfrastructure

# Fixed "latest" S3 keys (version-less "0.0.0" placeholder; pip reads the real
# version from the wheel metadata). Point at these so a Praktika/controller
# publish reaches this project without re-pinning; the pool user_data
# reinstalls them at boot.
_PRAKTIKA_CONTROLLER_WHL = "https://praktika-artifacts-eu-north-1.s3.amazonaws.com/packages/latest/praktika_controller-0.0.0-py3-none-any.whl"
_PRAKTIKA_WHL = "https://praktika-artifacts-eu-north-1.s3.amazonaws.com/packages/latest/praktika-0.0.0-py3-none-any.whl"


def _pool_user_data() -> str:
    # Boot-time install of the latest Praktika (and controller) wheels so a
    # wheel bump takes effect without rebuilding the AMI. Images are Ubuntu,
    # so the system-python controller install uses --ignore-installed (there
    # is no distro praktika_controller to uninstall); the base venv install
    # uses --force-reinstall to override the pinned wheel baked into the AMI.
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -xeuo pipefail",
            "",
            "# Update the controller if changed (to test new version w/o image rebuild)",
            f"python3.12 -m pip install --ignore-installed {_PRAKTIKA_CONTROLLER_WHL} --break-system-packages",
            "# Add any host customization you need above this line.",
            "/usr/local/bin/praktika-configure-cloudwatch-agent",
            "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -c file:/etc/praktika/amazon-cloudwatch-agent.json -s",
            (
                f"/opt/praktika/base-venvs/{PRAKTIKA_BASE_VENV}/bin/python "
                f"-m pip install --force-reinstall {_PRAKTIKA_WHL}"
            ),
            "systemctl enable --now praktika-controller",
            "",
        ]
    )


_POOL_USER_DATA = _pool_user_data()
MAC_OS_TAHOE_ARM_IMAGE_AMI = "ami-0cbd6d494543c15b3"


def _image_builders():
    image_recipe_version = "1.0.4"
    prebuilt_venvs = [
        # The `infrastructure` extra pulls Praktika's runtime deps
        # (boto3/PyJWT/cryptography/requests) automatically; pytest is
        # an optional extra the runner needs, so list it explicitly.
        # anthropic[bedrock] is the AI advisor SDK the orchestrator imports
        # lazily when AI_PROVIDER="bedrock"; baked into the shared venv so it's
        # present on the orchestrator (harmless on job runners).
        ImageBuilder.PrebuiltVenv(
            name=PRAKTIKA_BASE_VENV,
            packages=[
                "anthropic[bedrock]",
                f"praktika[infrastructure] @ {_PRAKTIKA_WHL}",
            ],
            description="Praktika runtime venv",
        ),
    ]
    custom_image_tests = [
        Components.create_image_test_component(
            name="project-image-test",
            commands=[
                "test -d /opt/praktika/work",
                "test -w /opt/praktika/work",
            ],
        ),
    ]
    return [
        Components.create_ubuntu_image_builder_config(
            name="ci-arm64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            components=custom_image_tests,
            instance_types=["t4g.small"],
        ),
        Components.create_ubuntu_image_builder_config(
            name="ci-x86_64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            components=custom_image_tests,
            instance_types=["t3.small"],
        ),
    ]


_GH_TOKEN_MINTER = Components.GitHubTokenMinter(
    permissions={
        "checks": "write",
        "contents": "write",
        "issues": "write",
        "metadata": "read",
        "pull_requests": "write",
        "statuses": "write",
    },
    repositories=[PROJECT_NAME],
)
_IMAGE_BUILDERS = _image_builders()
_IMAGE_BUILDERS_BY_NAME = {builder.name: builder for builder in _IMAGE_BUILDERS}


def _runner_s3_prefixes(name):
    """S3 access per pool type.

    Both share the CI cache (ci_ch_cache) and the runs/ protocol path
    (heartbeats + final-state). They differ on PR- vs REF-scoped data and on
    whether the compiler cache (ccache/sccache) is writable:

    - pr-* pools: write PR artifacts/reports + CI cache; read-only on the
      shared REFs and the compiler cache (PR builds must not pollute them,
      SCCACHE_S3_READ_ONLY=true).
    - non-pr pools (master/release): write the shared REFs, both caches, and
      runs. No access to PR-scoped prefixes.
    """
    if name.startswith("pr-"):
        read_write = [
            "clickhouse-builds/runs",
            "clickhouse-builds/PRs",
            "clickhouse-test-reports/PRs",
            "clickhouse-builds/ci_ch_cache",
            "clickhouse-test-reports-private/slack_feed",
        ]
        read_only = [
            "clickhouse-builds/REFs",
            "clickhouse-test-reports/REFs",
            "clickhouse-builds/ccache",
        ]
    else:
        read_write = [
            "clickhouse-builds/runs",
            "clickhouse-builds/REFs",
            "clickhouse-test-reports/REFs",
            "clickhouse-builds/ci_ch_cache",
            "clickhouse-builds/ccache",
            "clickhouse-test-reports-private/slack_feed",
        ]
        read_only = []
    return read_write, read_only


# SSM Parameter Store secrets read by jobs (CI DB, DockerHub, Azure, ccache).
# clickhouse-* names are project-namespaced idempotently; the rest are given as
# wildcard ARNs so they are used verbatim (a bare name would be prefixed to
# "clickhouse-<name>" and miss the real parameter). The GitHub App SSM secrets
# are intentionally excluded — only the token-minter Lambda needs those.
_RUNNER_SSM_PARAMETERS = [
    "clickhouse-dockerhub-registry",
    "clickhouse-test-stat-connection",
    "arn:aws:ssm:*:*:parameter/azure_connection_string",
    "arn:aws:ssm:*:*:parameter/chcache_password",
    # CI logs cluster credentials (host/password/etc.).
    "arn:aws:ssm:*:*:parameter/clickhouse_ci_logs*",
]


def _runner_pool(name, instance_type, max_size, volume_size_gb):
    image_builder_name = "ci-arm64-image" if "-arm-" in name else "ci-x86_64-image"
    read_write, read_only = _runner_s3_prefixes(name)
    return Components.RunnerPool(
        name=name,
        instance_type=instance_type,
        scaling=Components.RunnerPool.Scaling.Auto,
        size=0,
        max_size=max_size,
        volume_size_gb=volume_size_gb,
        image_builder=_IMAGE_BUILDERS_BY_NAME[image_builder_name],
        allowed_ssm_parameters=_RUNNER_SSM_PARAMETERS,
        allowed_secrets=[],
        allowed_s3_prefixes=read_write,
        allowed_s3_prefixes_readonly=read_only,
        allow_all_ssm_parameters=False,
        allow_all_secrets=False,
        allow_all_s3_prefixes=False,
        allow_ssm_debug=False,
        user_data=_POOL_USER_DATA,
    )


def _mac_runner_pool(
    name,
    availability_zones,
    instance_type,
    image_id,
    quantity_per_az,
    vpc_name,
):
    # macOS runners cannot autoscale (Apple licensing requires dedicated hosts
    # with a 24h minimum allocation), so they use a fixed-capacity
    # DedicatedRunnerPool instead of a RunnerPool. The interface otherwise
    # mirrors _runner_pool: name == SQS queue == runs_on label, with a scoped
    # instance role built from the S3/SSM opt-ins below. The subnet (per AZ)
    # and the "{vpc_name}-sg" security group resolve from `vpc_name` at deploy.
    read_write, read_only = _runner_s3_prefixes(name)
    return Components.DedicatedRunnerPool(
        name=name,
        instance_type=instance_type,
        availability_zones=availability_zones,
        quantity_per_az=quantity_per_az,
        image_id=image_id,
        vpc_name=vpc_name,
        # Legacy GitHub-runner labels (github:runner-type). Joined with ","
        # into the single tag; runner-init expands them into runner labels.
        # Underscore forms, distinct from the dashed pool/queue name.
        runner_type=["pr-macos-m2"],
        key_name="awswork",
        user_data_file="./ci/infra/scripts/user_data_macos.txt",
        root_volume_size_gb=100,
        root_volume_type="gp3",
        root_volume_encrypted=True,
        # The generated instance role gets the same project S3/SSM the Linux
        # runners use, plus the bootstrap bucket and GitHub runner registration
        # token the macOS runner-init reads. Those two live outside the project
        # namespace, so they are given as full ARNs to bypass name-prefixing.
        allowed_ssm_parameters=[
            *_RUNNER_SSM_PARAMETERS,
            "arn:aws:ssm:*:*:parameter/github_runner_registration_token",
        ],
        allowed_secrets=[],
        # `gh_actions/` is where the legacy runner-init uploads failure logs.
        allowed_s3_prefixes=[
            *read_write,
            "clickhouse-test-reports-private/gh_actions",
        ],
        allowed_s3_prefixes_readonly=[
            *read_only,
            "arn:aws:s3:::github-runners-data",
            "arn:aws:s3:::github-runners-data/cloud-init/*",
        ],
        allow_all_ssm_parameters=False,
        allow_all_secrets=False,
        allow_all_s3_prefixes=False,
        allow_ssm_debug=True,
        # Raw statements the allow_* lists can't express, mirroring the legacy
        # untrusted_runner role the GitHub-mode runners use.
        iam_statements=[
            {
                # runner-init reads its own instance tags via the boto3 EC2
                # resource (.tags -> DescribeInstances/DescribeTags).
                "Sid": "RunnerInitDescribeSelf",
                "Effect": "Allow",
                "Action": ["ec2:DescribeInstances", "ec2:DescribeTags"],
                "Resource": "*",
            },
            {
                # SecureString SSM params (e.g. the runner registration token)
                # are decrypted via KMS on GetParameter(WithDecryption=True).
                "Sid": "RunnerKmsDecrypt",
                "Effect": "Allow",
                "Action": ["kms:Decrypt"],
                "Resource": "*",
            },
        ],
    )


# Grants the orchestrator EC2 role Bedrock runtime inference so the AI advisor
# (Workflow.ai_orchestrator with provider="bedrock") can call InvokeModel.
# Appended into the shared WorkflowOrchestratorAccess inline policy via ext.
_ORCHESTRATOR_BEDROCK_IAM_STATEMENT = {
    "Sid": "BedrockRuntimeInference",
    "Effect": "Allow",
    "Action": ["bedrock:InvokeModel"],
    "Resource": "*",
}


PROJECTS = [
    CloudInfrastructure.Config(
        name=PROJECT_NAME,
        min_praktika_version="0.1.9",
        vpcs=[
            # Primary/default VPC (us-east-1): all Linux pools and image
            # builders bind to this one implicitly.
            VPC.Config(
                subnets=[
                    VPC.Subnet(availability_zone="us-east-1a"),
                ],
            ),
            # Dedicated VPC for the macOS pool in ap-southeast-2. Referenced
            # explicitly by name from the DedicatedRunnerPool.
            VPC.Config(
                name="macos-ap-southeast-2",
                region="ap-southeast-2",
                subnets=[
                    VPC.Subnet(availability_zone="ap-southeast-2b"),
                ],
            ),
        ],
        storages=[
            # Storage.Config(
            #     name="artifacts-us-east-1",
            #     retention_days=30,
            #     public=True,
            # ),
        ],
        report_pages=[Components.report_page_config],
        # TODO: migrate from tf configs
        # image_builders=_IMAGE_BUILDERS,
        # github_token_minters=[_GH_TOKEN_MINTER],
        # orchestrator_pool=Components.OrchestratorPool(
        #     instance_type="t4g.large",
        #     scaling=Components.OrchestratorPool.Scaling.Auto,
        #     size=0,
        #     max_size=50,
        #     volume_size_gb=40,
        #     capacity_reserve=0,
        #     image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
        #     ext={
        #         'allowed_users': ['maxknv'],
        #         'iam_statements': [_ORCHESTRATOR_BEDROCK_IAM_STATEMENT],
        #     },
        #     user_data=_POOL_USER_DATA,
        # ),
        # runner_pools=[
        #     _runner_pool("pr-amd-tiny", "t3.large", 40, 60),
        #     _runner_pool("pr-amd-small", "m7i.2xlarge", 300, 150),
        #     _runner_pool("pr-amd-small-cpu", "c7i.4xlarge", 200, 150),
        #     _runner_pool("pr-amd-small-mem", "r7i.2xlarge", 200, 150),
        #     _runner_pool("pr-amd-medium", "m7i.4xlarge", 1200, 200),
        #     _runner_pool("pr-amd-medium-cpu", "c7i.8xlarge", 200, 200),
        #     _runner_pool("pr-amd-medium-mem", "r7i.4xlarge", 200, 200),
        #     _runner_pool("pr-amd-large", "m7i.8xlarge", 200, 150),
        #     _runner_pool("pr-arm-tiny", "t4g.large", 20, 60),
        #     _runner_pool("pr-arm-small", "m8g.2xlarge", 200, 150),
        #     _runner_pool("pr-arm-small-cpu", "c8g.4xlarge", 200, 150),
        #     _runner_pool("pr-arm-small-mem", "r8g.2xlarge", 200, 150),
        #     _runner_pool("pr-arm-medium", "m8g.4xlarge", 800, 200),
        #     _runner_pool("pr-arm-medium-cpu", "c8g.8xlarge", 200, 200),
        #     _runner_pool("pr-arm-medium-mem", "r8g.4xlarge", 200, 200),
        #     _runner_pool("pr-arm-large", "m8g.8xlarge", 200, 200),
        # ],
        # macOS runners: fixed-capacity dedicated-host pools (no autoscaling).
        dedicated_runner_pools=[
            _mac_runner_pool(
                name="pr-macos-m2",
                availability_zones=["ap-southeast-2b"],
                instance_type="mac2-m2pro.metal",
                image_id=MAC_OS_TAHOE_ARM_IMAGE_AMI,
                quantity_per_az=10,
                vpc_name="macos-ap-southeast-2",
            ),
        ],
        # TODO: make self-contained component SlackApp
        # lambda_functions=[*CloudInfrastructure.SLACK_APP_LAMBDAS],
    )
]
