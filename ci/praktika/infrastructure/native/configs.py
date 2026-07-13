import os

from pathlib import Path

from praktika.infrastructure.lambda_function import Lambda
from praktika.infrastructure.report_page import ReportPage
from praktika.settings import Settings

# SSM paths AWS maintains with the latest AL2023 AMI IDs per region
_AL2023_ARM64_SSM_PATH = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-arm64"
_AL2023_X86_64_SSM_PATH = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-x86_64"
_UBUNTU_24_04_ARM64_SSM_PATH = "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
_UBUNTU_24_04_X86_64_SSM_PATH = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"


def _resolve_ssm_ami(region: str, path: str, label: str) -> str:
    from praktika.infrastructure._utils import aws_client

    ssm = aws_client("ssm", region, "ami-lookup")
    value = ssm.get_parameter(Name=path)["Parameter"]["Value"]
    print(f"Resolved {label} AMI for {region}: {value}")
    return value


def resolve_al2023_arm64_ami(region: str) -> str:
    """Resolve the latest AL2023 ARM64 AMI ID for the given region via AWS SSM."""
    return _resolve_ssm_ami(region, _AL2023_ARM64_SSM_PATH, "AL2023 ARM64")


def resolve_al2023_x86_64_ami(region: str) -> str:
    """Resolve the latest AL2023 x86_64 AMI ID for the given region via AWS SSM."""
    return _resolve_ssm_ami(region, _AL2023_X86_64_SSM_PATH, "AL2023 x86_64")


def resolve_ubuntu_24_04_arm64_ami(region: str) -> str:
    """Resolve the latest Ubuntu 24.04 ARM64 AMI ID for the given region via AWS SSM."""
    return _resolve_ssm_ami(region, _UBUNTU_24_04_ARM64_SSM_PATH, "Ubuntu 24.04 ARM64")


def resolve_ubuntu_24_04_x86_64_ami(region: str) -> str:
    """Resolve the latest Ubuntu 24.04 x86_64 AMI ID for the given region via AWS SSM."""
    return _resolve_ssm_ami(region, _UBUNTU_24_04_X86_64_SSM_PATH, "Ubuntu 24.04 x86_64")

ORCHESTRATOR_ROLE_NAME = "workflow-orchestrator-role"
ORCHESTRATOR_INSTANCE_PROFILE_NAME = "workflow-orchestrator-profile"

report_page_config = ReportPage.Config(
    path=str(Path(__file__).parent.parent.parent / "praktika.html"),
)

GH_TRIGGER_ROLE_NAME = "gh-trigger-role"
GH_TRIGGER_WEBHOOK_SECRET_NAME = "gh-trigger-webhook-secret"

CIDB_ROLE_NAME = "cidb-role"
CIDB_INSTANCE_PROFILE_NAME = "cidb-profile"
CIDB_ADMIN_PASSWORD_SECRET_NAME = "cidb-admin-password"

lambda_gh_trigger_config = Lambda.Config(
    name="gh-trigger",
    path=f"{os.path.dirname(__file__)}/lambda_gh_trigger.py",
    handler="lambda_gh_trigger.lambda_handler",
    role_name=GH_TRIGGER_ROLE_NAME,
    secrets={
        GH_TRIGGER_WEBHOOK_SECRET_NAME: "GH_WEBHOOK_SECRET",
    },
    # S3_BUCKET points the lambda at the per-run S3 prefix where it writes
    # cancel-request and scoped cancel-before flags. Same artifacts bucket
    # the orchestrator and runners use.
    environments={
        "S3_BUCKET": Settings.S3_ARTIFACT_BUCKET or "",
    },
    timeout_ms=10 * 1000,
    memory_size_mb=128,
    api_gateway=True,
)
