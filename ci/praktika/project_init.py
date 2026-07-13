from __future__ import annotations

import dataclasses
import configparser
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Dict, List, Optional, Set

from .interactive import UserPrompt
from .version import compat_version, current_praktika_version


PRAKTIKA_MARKERS = {
    "settings": Path("ci/settings/settings.py"),
    "workflows": Path("ci/workflows"),
    "infrastructure": Path("ci/infrastructure/projects.py"),
}


@dataclasses.dataclass
class InitAnswers:
    project_name: str
    main_branch: str
    aws_region: str
    availability_zone: str
    aws_account_id: str
    aws_profile: str
    is_oss: bool = False
    image_base: str = "awslinux"
    enable_ai_capabilities: bool = False

    @property
    def project_slug(self) -> str:
        # Use "_" (not "-") as the intra-slug separator. The slug is used as
        # the "{slug}-" resource-name prefix, so a "-" inside the slug would
        # let one project's scoped IAM wildcard (e.g. "clickhouse-*") also
        # match another project's resources (e.g. "clickhouse-private-*").
        slug = re.sub(r"[^a-z0-9]+", "_", self.project_name.strip().lower())
        slug = re.sub(r"_{2,}", "_", slug).strip("_")
        if not slug:
            raise ValueError("Project name must normalize to a non-empty slug")
        return slug

    @property
    def artifact_bucket(self) -> str:
        return f"{self.project_slug}-{self.artifact_storage_name}"

    @property
    def artifact_storage_name(self) -> str:
        if self.is_oss:
            return f"artifacts-{self.aws_region}"
        return "artifacts"

    @property
    def image_builder_factory(self) -> str:
        if self.image_base == "ubuntu":
            return "create_ubuntu_image_builder_config"
        return "create_awslinux_image_builder_config"

    @property
    def gh_auth_lambda_name(self) -> str:
        return f"{self.project_slug}-gh-token"

    @property
    def gh_app_secret_name(self) -> str:
        return f"{self.project_slug}-gh-app"

    @property
    def vpc_name(self) -> str:
        return f"{self.project_slug}-ci"


def find_git_repo_root(start: Optional[Path] = None) -> Optional[Path]:
    cwd = Path(start or Path.cwd()).resolve()
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return Path(result.stdout.strip()).resolve()


def detect_default_branch(start: Optional[Path] = None) -> str:
    cwd = Path(start or Path.cwd()).resolve()
    commands = [
        ["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
    ]
    for command in commands:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            continue
        branch = result.stdout.strip()
        if not branch or branch == "HEAD":
            continue
        if branch.startswith("origin/"):
            branch = branch.removeprefix("origin/")
        if branch:
            return branch
    return "main"


def is_git_repo_root(path: Optional[Path] = None) -> bool:
    cwd = Path(path or Path.cwd()).resolve()
    root = find_git_repo_root(cwd)
    return root is not None and root == cwd


def has_nested_git_repositories(path: Optional[Path] = None) -> bool:
    root = Path(path or Path.cwd()).resolve()
    try:
        for child in root.iterdir():
            if not child.is_dir():
                continue
            if child.name.startswith("."):
                continue
            if (child / ".git").exists():
                return True
    except OSError:
        return False
    return False


def detect_praktika_paths(root: Optional[Path] = None) -> Dict[str, bool]:
    repo_root = Path(root or Path.cwd()).resolve()
    return {
        name: (repo_root / rel_path).exists()
        for name, rel_path in PRAKTIKA_MARKERS.items()
    }


def has_praktika_project_files(root: Optional[Path] = None) -> bool:
    return any(detect_praktika_paths(root).values())


def _validate_project_name(value: str) -> bool:
    return bool(value.strip())


def _validate_aws_region(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z]{2}-[a-z]+-\d", value.strip()))


def _validate_availability_zone(value: str) -> bool:
    return bool(re.fullmatch(r"[a-z]{2}-[a-z]+-\d[a-z]", value.strip()))


def _validate_aws_account_id(value: str) -> bool:
    return bool(re.fullmatch(r"\d{12}", value.strip()))


def _validate_bucket_name(value: str) -> bool:
    return bool(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?", value.strip())
    )


def _normalize_image_base(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-").replace(" ", "-")
    aliases = {
        "ubuntu": "ubuntu",
        "awslinux": "awslinux",
        "aws-linux": "awslinux",
        "amazonlinux": "awslinux",
        "amazon-linux": "awslinux",
    }
    return aliases.get(normalized, "")


def _validate_image_base(value: str) -> bool:
    return bool(_normalize_image_base(value))


def _prompt_image_base() -> str:
    image_base = UserPrompt.get_string(
        "Image base for CI runners (ubuntu or awslinux)",
        validator=_validate_image_base,
        default="awslinux",
    )
    normalized = _normalize_image_base(image_base)
    if not normalized:
        raise ValueError(f"Invalid image base [{image_base}]")
    return normalized


def detect_aws_profiles() -> Set[str]:
    files = [
        Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws/config")),
        Path(
            os.environ.get(
                "AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws/credentials"
            )
        ),
    ]
    profiles: Set[str] = set()
    parser = configparser.RawConfigParser()

    for file_path in files:
        if not file_path.is_file():
            continue
        parser.read(file_path, encoding="utf8")
        for section in parser.sections():
            if section == "default":
                profiles.add("default")
            elif section.startswith("profile "):
                profiles.add(section.removeprefix("profile ").strip())
            else:
                profiles.add(section.strip())
        parser.clear()

    return {profile for profile in profiles if profile}


def detect_aws_account_ids() -> Set[str]:
    files = [
        Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws/config")),
        Path(
            os.environ.get(
                "AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws/credentials"
            )
        ),
    ]
    account_ids: Set[str] = set()
    parser = configparser.RawConfigParser()

    for file_path in files:
        if not file_path.is_file():
            continue
        parser.read(file_path, encoding="utf8")
        for section in parser.sections():
            for _, raw_value in parser.items(section):
                value = (raw_value or "").strip()
                if re.fullmatch(r"\d{12}", value):
                    account_ids.add(value)
        parser.clear()

    return account_ids


def detect_aws_profile_account_ids() -> Dict[str, Set[str]]:
    files = [
        Path(os.environ.get("AWS_CONFIG_FILE", Path.home() / ".aws/config")),
        Path(
            os.environ.get(
                "AWS_SHARED_CREDENTIALS_FILE", Path.home() / ".aws/credentials"
            )
        ),
    ]
    profile_account_ids: Dict[str, Set[str]] = {}
    parser = configparser.RawConfigParser()

    for file_path in files:
        if not file_path.is_file():
            continue
        parser.read(file_path, encoding="utf8")
        for section in parser.sections():
            if section == "default":
                profile = "default"
            elif section.startswith("profile "):
                profile = section.removeprefix("profile ").strip()
            else:
                profile = section.strip()
            if not profile:
                continue
            for _, raw_value in parser.items(section):
                value = (raw_value or "").strip()
                if re.fullmatch(r"\d{12}", value):
                    profile_account_ids.setdefault(profile, set()).add(value)
        parser.clear()

    return profile_account_ids


def _validate_aws_profile(value: str) -> bool:
    profile = value.strip()
    if not profile:
        return False
    profiles = detect_aws_profiles()
    if not profiles:
        return True
    return profile in profiles


def _prompt_aws_profile(default: str = "default") -> str:
    profiles = sorted(detect_aws_profiles())
    prompt = f"\nAWS profile name"
    if profiles:
        prompt += f" (options: {', '.join(profiles)}"
        if default:
            prompt += f"; default: {default}"
        prompt += ")"
    if default:
        if not profiles:
            prompt += f" (default: {default})"
    prompt += ": "

    while True:
        choice = UserPrompt._safe_input(prompt).strip()
        if not choice and default:
            choice = default
        if _validate_aws_profile(choice):
            return choice

        if profiles:
            print(
                "ERROR: Unknown AWS profile "
                f"[{choice}]. Available profiles: {', '.join(profiles)}"
            )
        else:
            print("ERROR: Invalid AWS profile name.")


def _prompt_aws_account_id(profile: str = "") -> str:
    profile_account_ids = detect_aws_profile_account_ids()
    account_ids = sorted(profile_account_ids.get(profile, set()))
    if len(account_ids) == 1:
        print(
            f"Using AWS account ID [{account_ids[0]}] from local config for profile [{profile}]"
        )
        return account_ids[0]
    if not account_ids:
        account_ids = sorted(detect_aws_account_ids())
    default = account_ids[0] if len(account_ids) == 1 else ""
    prompt = "\nAWS account ID"
    if account_ids:
        prompt += f" (options: {', '.join(account_ids)}"
        if default:
            prompt += f"; default: {default}"
        prompt += ")"
    prompt += ": "

    while True:
        choice = UserPrompt._safe_input(prompt).strip()
        if not choice and default:
            choice = default
        if _validate_aws_account_id(choice):
            return choice

        if account_ids:
            print(
                "ERROR: Invalid AWS account ID "
                f"[{choice}]. Available account IDs: {', '.join(account_ids)}"
            )
        else:
            print("ERROR: Invalid AWS account ID.")


def _prompt_for_answers(
    root: Path, components: Optional[List[str]] = None
) -> InitAnswers:
    selected_components = set(
        components
        if components is not None
        else ("settings", "workflows", "infrastructure")
    )
    repo_name = root.name
    default_branch = detect_default_branch(root)
    project_name = repo_name
    main_branch = UserPrompt.get_string(
        "Default branch name",
        validator=_validate_project_name,
        default=default_branch,
    ).strip()
    aws_region = UserPrompt.get_string(
        "AWS region (for example us-east-1)",
        validator=_validate_aws_region,
    ).strip()
    availability_zone = UserPrompt.get_string(
        "Primary availability zone",
        validator=_validate_availability_zone,
        default=f"{aws_region}a",
    ).strip()
    aws_profile = _prompt_aws_profile(default="default")
    aws_account_id = _prompt_aws_account_id(profile=aws_profile)
    is_oss = False
    if selected_components & {"settings", "infrastructure"}:
        is_oss = UserPrompt.confirm(
            "Is this an OSS project that should use public artifact storage?"
        )
    image_base = "awslinux"
    if "infrastructure" in selected_components:
        image_base = _prompt_image_base()
    enable_ai_capabilities = False
    if "workflows" in selected_components:
        enable_ai_capabilities = UserPrompt.confirm(
            "Enable AI capabilities for the pull request workflow?"
        )
    return InitAnswers(
        project_name=project_name,
        main_branch=main_branch,
        aws_region=aws_region,
        availability_zone=availability_zone,
        aws_account_id=aws_account_id,
        aws_profile=aws_profile,
        is_oss=is_oss,
        image_base=image_base,
        enable_ai_capabilities=enable_ai_capabilities,
    )


def _settings_template(answers: InitAnswers) -> str:
    artifact_bucket_expr = (
        'f"{PROJECT_SLUG}-artifacts-{AWS_REGION}"'
        if answers.is_oss
        else 'f"{PROJECT_SLUG}-artifacts"'
    )
    return textwrap.dedent(
        f"""\
        class RunnerLabels:
            SMALL_ARM = "arm-small"
            SMALL_AMD = "amd-small"
            MEDIUM_ARM = "arm-medium"
            MEDIUM_AMD = "amd-medium"


        PROJECT_NAME = "{answers.project_name}"
        PROJECT_SLUG = "{answers.project_slug}"
        MAIN_BRANCH = "{answers.main_branch}"

        CI_CONFIG_RUNS_ON = [RunnerLabels.SMALL_ARM]

        AWS_REGION = "{answers.aws_region}"
        AWS_PROFILE = "{answers.aws_profile}"

        S3_ARTIFACT_BUCKET = {artifact_bucket_expr}
        S3_REPORT_BUCKET = S3_ARTIFACT_BUCKET
        CACHE_S3_PATH = f"{{S3_ARTIFACT_BUCKET}}/ci_cache"
        ENABLE_SUBMODULE_CACHE = True
        S3_BUCKET_TO_HTTP_ENDPOINT = {{
            S3_REPORT_BUCKET: f"{{S3_REPORT_BUCKET}}.s3.amazonaws.com",
        }}

        USE_CUSTOM_GH_AUTH = True
        GH_AUTH_LAMBDA_NAME = f"{{PROJECT_SLUG}}-gh-token"
        GH_AUTH_LAMBDA_REGION = AWS_REGION
        PRAKTIKA_BASE_VENV = "praktika-runtime-{current_praktika_version()}"

        """
    )


def _pull_request_workflow_template(answers: InitAnswers) -> str:
    command = 'python3 -c \'print("hello from praktika")\''
    ai_orchestrator = ""
    if answers.enable_ai_capabilities:
        ai_orchestrator = textwrap.indent(
            textwrap.dedent(
                """\
                ai_orchestrator=Workflow.OrchestratorAI.Config(
                    enabled=True,
                    provider="bedrock",
                    model="global.anthropic.claude-sonnet-5",
                ),
                """
            ),
            "        ",
        )
    return textwrap.dedent(
        f"""\
        from praktika import Job, Workflow
        from ci.settings.settings import RunnerLabels


        WORKFLOWS = [
            Workflow.Config(
                name="Pull Request CI",
                event=Workflow.Event.PULL_REQUEST,
                base_branches=["{answers.main_branch}"],
{ai_orchestrator if ai_orchestrator else ""}
                jobs=[
                    Job.Config(
                        name="Smoke Test",
                        runs_on=[RunnerLabels.SMALL_ARM],
                        command={command!r},
                    ),
                ],
                enable_cache=True,
                enable_report=True,
                enable_gh_summary_comment=True,
                enable_exit_code_result=True,
            )
        ]
        """
    )


def _main_ci_workflow_template(answers: InitAnswers) -> str:
    command = 'python3 -c \'print("hello from main ci")\''
    return textwrap.dedent(
        f"""\
        from praktika import Job, Workflow
        from ci.settings.settings import RunnerLabels


        WORKFLOWS = [
            Workflow.Config(
                name="Main CI",
                event=Workflow.Event.PUSH,
                branches=["{answers.main_branch}"],
                jobs=[
                    Job.Config(
                        name="Smoke Test",
                        runs_on=[RunnerLabels.SMALL_ARM],
                        command={command!r},
                    ),
                ],
                enable_cache=True,
                enable_report=True,
                enable_exit_code_result=True,
            )
        ]
        """
    )


def _infrastructure_template(answers: InitAnswers) -> str:
    optional_ai_package = ""
    optional_ai_permissions = ""
    optional_ai_ext = ""
    if answers.enable_ai_capabilities:
        optional_ai_package = '                "anthropic[bedrock]",\n'
        optional_ai_permissions = """\
        _ORCHESTRATOR_BEDROCK_IAM_STATEMENT = {
            "Sid": "BedrockRuntimeInference",
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel"],
            "Resource": "*",
        }
"""
        optional_ai_ext = '\n                ext={"iam_statements": [_ORCHESTRATOR_BEDROCK_IAM_STATEMENT]},'
    return textwrap.dedent(
        f"""\
        from ci.settings.settings import PROJECT_NAME, PRAKTIKA_BASE_VENV
        from praktika.infrastructure import Components, ImageBuilder, Storage, VPC
        from praktika.infrastructure.cloud import CloudInfrastructure


        # until published in pip
        _PRAKTIKA_CONTROLLER_WHL = "https://praktika-artifacts-eu-north-1.s3.amazonaws.com/packages/praktika_controller-0.1.4-py3-none-any.whl"
        # Floating compat alias: the latest backwards-compatible patch in the
        # {compat_version(current_praktika_version())} branch, so the project picks up BC bug fixes
        # without re-pinning on every Praktika release.
        _PRAKTIKA_WHL = "https://praktika-artifacts-eu-north-1.s3.amazonaws.com/packages/{compat_version(current_praktika_version())}/praktika-0.0.0-py3-none-any.whl"


        def _image_builders():
            image_recipe_version = "1.0.0"
            prebuilt_venvs = [
                # The `infrastructure` extra pulls Praktika's runtime deps
                # (boto3/PyJWT/cryptography/requests) automatically; pytest is
                # an optional extra the runner needs, so list it explicitly.
                ImageBuilder.PrebuiltVenv(
                    name=PRAKTIKA_BASE_VENV,
                    packages=[
                        "pytest>=7.0.0",
                        "pytest-reportlog>=0.4.0",
{optional_ai_package}                        f"praktika[infrastructure] @ {{_PRAKTIKA_WHL}}",
                    ],
                    description="Praktika runtime venv (infrastructure extra + pytest)",
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
                Components.{answers.image_builder_factory}(
                    name="ci-arm64-image",
                    version=image_recipe_version,
                    controller_package=_PRAKTIKA_CONTROLLER_WHL,
                    prebuilt_venvs=prebuilt_venvs,
                    components=custom_image_tests,
                    instance_types=["t4g.small"],
                ),
                Components.{answers.image_builder_factory}(
                    name="ci-x86_64-image",
                    version=image_recipe_version,
                    controller_package=_PRAKTIKA_CONTROLLER_WHL,
                    prebuilt_venvs=prebuilt_venvs,
                    components=custom_image_tests,
                    instance_types=["t3.small"],
                ),
            ]


        _GH_TOKEN_MINTER = Components.GitHubTokenMinter(
            permissions={{
                "checks": "write",
                "contents": "write",
                "issues": "write",
                "metadata": "read",
                "pages": "write",
                "pull_requests": "write",
                "statuses": "write",
            }},
            repositories=[PROJECT_NAME],
        )
        _IMAGE_BUILDERS = _image_builders()
        _IMAGE_BUILDERS_BY_NAME = {{builder.name: builder for builder in _IMAGE_BUILDERS}}
{optional_ai_permissions}

        PROJECTS = [
            CloudInfrastructure.Config(
                name=PROJECT_NAME,
                min_praktika_version="{current_praktika_version()}",
                vpcs=[
                    VPC.Config(
                        subnets=[
                            VPC.Subnet(availability_zone="{answers.availability_zone}"),
                        ],
                    )
                ],
                storages=[
                    Storage.Config(
                        name="{answers.artifact_storage_name}",
                        retention_days=30,
                        public={answers.is_oss},
                    ),
                ],
                report_pages=[Components.report_page_config],
                image_builders=_IMAGE_BUILDERS,
                github_token_minters=[_GH_TOKEN_MINTER],
                orchestrator_pool=Components.OrchestratorPool(
                    instance_type="t4g.small",
                    scaling=Components.OrchestratorPool.Scaling.Auto,
                    size=0,
                    max_size=50,
                    volume_size_gb=100,
                    capacity_reserve=1,
                    image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],{optional_ai_ext}
                ),
                runner_pools=[
                    Components.RunnerPool(
                        name="arm-small",
                        instance_type="t4g.medium",
                        scaling=Components.RunnerPool.Scaling.Auto,
                        size=0,
                        max_size=50,
                        volume_size_gb=100,
                        image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
                        allowed_ssm_parameters=[],
                        allowed_secrets=[],
                        allowed_s3_prefixes=["{answers.artifact_storage_name}"],
                        allow_all_ssm_parameters={not answers.is_oss},
                        allow_all_secrets={not answers.is_oss},
                        allow_all_s3_prefixes={not answers.is_oss},
                        allow_ssm_debug=False,
                    ),
                    Components.RunnerPool(
                        name="amd-small",
                        instance_type="t3.medium",
                        scaling=Components.RunnerPool.Scaling.Auto,
                        size=0,
                        max_size=50,
                        volume_size_gb=100,
                        image_builder=_IMAGE_BUILDERS_BY_NAME["ci-x86_64-image"],
                        allowed_ssm_parameters=[],
                        allowed_secrets=[],
                        allowed_s3_prefixes=["{answers.artifact_storage_name}"],
                        allow_all_ssm_parameters={not answers.is_oss},
                        allow_all_secrets={not answers.is_oss},
                        allow_all_s3_prefixes={not answers.is_oss},
                        allow_ssm_debug=False,
                    ),
                    Components.RunnerPool(
                        name="arm-medium",
                        instance_type="c7g.4xlarge",
                        scaling=Components.RunnerPool.Scaling.Auto,
                        size=0,
                        max_size=50,
                        volume_size_gb=100,
                        image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
                        allowed_ssm_parameters=[],
                        allowed_secrets=[],
                        allowed_s3_prefixes=["{answers.artifact_storage_name}"],
                        allow_all_ssm_parameters={not answers.is_oss},
                        allow_all_secrets={not answers.is_oss},
                        allow_all_s3_prefixes={not answers.is_oss},
                        allow_ssm_debug=False,
                    ),
                    Components.RunnerPool(
                        name="amd-medium",
                        instance_type="c7a.4xlarge",
                        scaling=Components.RunnerPool.Scaling.Auto,
                        size=0,
                        max_size=50,
                        volume_size_gb=100,
                        image_builder=_IMAGE_BUILDERS_BY_NAME["ci-x86_64-image"],
                        allowed_ssm_parameters=[],
                        allowed_secrets=[],
                        allowed_s3_prefixes=["{answers.artifact_storage_name}"],
                        allow_all_ssm_parameters={not answers.is_oss},
                        allow_all_secrets={not answers.is_oss},
                        allow_all_s3_prefixes={not answers.is_oss},
                        allow_ssm_debug=False,
                    ),
                ],
            )
        ]
        """
    ).lstrip()


def _render_files(answers: InitAnswers) -> Dict[str, Dict[Path, str] | str]:
    return {
        "settings": _settings_template(answers),
        "workflows": {
            PRAKTIKA_MARKERS["workflows"] / "pull_request.py": _pull_request_workflow_template(answers),
            PRAKTIKA_MARKERS["workflows"] / "main_ci.py": _main_ci_workflow_template(answers),
        },
        "infrastructure": _infrastructure_template(answers),
    }


def _component_file_targets() -> Dict[str, List[Path]]:
    return {
        "settings": [PRAKTIKA_MARKERS["settings"]],
        "workflows": [
            PRAKTIKA_MARKERS["workflows"] / "pull_request.py",
            PRAKTIKA_MARKERS["workflows"] / "main_ci.py",
        ],
        "infrastructure": [PRAKTIKA_MARKERS["infrastructure"]],
    }


def _pick_components(root: Path) -> List[str]:
    existing = detect_praktika_paths(root)
    selected = []
    labels = {
        "settings": "ci/settings/settings.py",
        "workflows": "ci/workflows/",
        "infrastructure": "ci/infrastructure/projects.py",
    }
    descriptions = {
        "settings": (
            "Defines runner labels, AWS settings, and Praktika project defaults."
        ),
        "workflows": (
            "Creates starter pull request and push workflow configs."
        ),
        "infrastructure": (
            "Required only for standalone Praktika CI (not GitHub Actions), and only if this repo should manage the infrastructure."
        ),
    }
    for component in ("settings", "workflows", "infrastructure"):
        if component in {"settings", "workflows"} and not existing[component]:
            selected.append(component)
            continue
        if existing[component]:
            question = (
                f"Remove existing {labels[component]} and regenerate it? "
                f"{descriptions[component]}"
            )
        else:
            question = f"Create {labels[component]}? {descriptions[component]}"
        if UserPrompt.confirm(question):
            selected.append(component)
    return selected


def scaffold_project(root: Path, answers: InitAnswers, components: List[str]) -> List[Path]:
    written = []
    rendered = _render_files(answers)
    targets = _component_file_targets()
    for component in components:
        component_targets = targets[component]
        component_rendered = rendered[component]
        if isinstance(component_rendered, dict):
            for rel_path in component_targets:
                target = root / rel_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(component_rendered[rel_path], encoding="utf8")
                written.append(target)
            continue

        for rel_path in component_targets:
            target = root / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(component_rendered, encoding="utf8")
            written.append(target)
    return written


def print_scaffold_summary(
    written_files: List[Path], root: Path, answers: InitAnswers
) -> None:
    if not written_files:
        print("No files were created.")
        return

    print("\nInitialized Praktika project with:")
    for path in written_files:
        print(f"  - {path.relative_to(root)}")

    print(
        textwrap.dedent(
            """

            Next steps:
              1. Create the GitHub App secret `"""
            + answers.gh_app_secret_name
            + """` in AWS Secrets Manager with keys `app-id`, `app-key`, and `app-installation-id`.
              2. Review ci/settings/settings.py and ci/infrastructure/projects.py for your AWS naming and sizing choices.
              3. Deploy the infrastructure with `python3 -m praktika infrastructure --deploy`.
            """
        ).rstrip()
    )


def run_init_interactive(root: Optional[Path] = None) -> List[Path]:
    repo_root = Path(root or Path.cwd()).resolve()
    components = _pick_components(repo_root)
    if not components:
        print("Initialization cancelled: no project files selected.")
        return []
    answers = _prompt_for_answers(repo_root, components)
    written = scaffold_project(repo_root, answers, components)
    print_scaffold_summary(written, repo_root, answers)
    return written


def prompt_init_from_repo_root(root: Optional[Path] = None) -> bool:
    repo_root = Path(root or Path.cwd()).resolve()
    print(
        "No Praktika project files were found in this git repository root.\n"
        "Praktika expects ci/settings/settings.py, ci/workflows/, and ci/infrastructure/projects.py."
    )

    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print("Run `praktika init` to scaffold a starter project.")
        return False

    if not UserPrompt.confirm("Initialize a new Praktika project here?"):
        return False

    run_init_interactive(repo_root)
    return True
