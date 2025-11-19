import glob
from itertools import chain
from pathlib import Path

from praktika import Artifact, Job

from . import Workflow
from .mangle import _get_workflows
from .settings import GHRunners, Settings


class Validator:
    @classmethod
    def validate(cls):
        print("---Start validating Pipeline and settings---")

        if Settings.DISABLED_WORKFLOWS:
            for file in Settings.DISABLED_WORKFLOWS:
                cls.evaluate_check_simple(
                    Path(file).is_file()
                    or Path(f"{Settings.WORKFLOWS_DIRECTORY}/{file}").is_file(),
                    f"Setting DISABLED_WORKFLOWS has non-existing workflow file [{file}]",
                )

        if Settings.ENABLED_WORKFLOWS:
            for file in Settings.ENABLED_WORKFLOWS:
                cls.evaluate_check_simple(
                    Path(file).is_file()
                    or Path(f"{Settings.WORKFLOWS_DIRECTORY}/{file}").is_file(),
                    f"Setting ENABLED_WORKFLOWS has non-existing workflow file [{file}]",
                )

        if Settings.USE_CUSTOM_GH_AUTH:
            cls.evaluate_check_simple(
                Settings.SECRET_GH_APP_ID and Settings.SECRET_GH_APP_PEM_KEY,
                f"Setting SECRET_GH_APP_ID and SECRET_GH_APP_PEM_KEY must be provided with USE_CUSTOM_GH_AUTH == True",
            )

        workflows = _get_workflows(_for_validation_check=True)
        for workflow in workflows:
            print(f"Validating workflow [{workflow.name}]")
            if Settings.USE_CUSTOM_GH_AUTH and workflow.enable_report:
                secret = workflow.get_secret(Settings.SECRET_GH_APP_ID)
                cls.evaluate_check(
                    bool(secret),
                    f"Secret [{Settings.SECRET_GH_APP_ID}] must be configured for workflow",
                    workflow.name,
                )
                secret = workflow.get_secret(Settings.SECRET_GH_APP_PEM_KEY)
                cls.evaluate_check(
                    bool(secret),
                    f"Secret [{Settings.SECRET_GH_APP_PEM_KEY}] must be configured for workflow",
                    workflow.name,
                )

            for job in workflow.jobs:
                cls.evaluate_check(
                    isinstance(job, Job.Config),
                    f"Invalid job type [{job}]: type [{type(job)}]",
                    workflow.name,
                )
                cls.evaluate_check(
                    job.runs_on
                    and isinstance(job.runs_on, list)
                    or isinstance(job.runs_on, tuple),
                    f"Invalid Job.Config.runs_on [{job.runs_on}] for [{job.name}]",
                    workflow.name,
                )
                cls.evaluate_check(
                    "PARAMETER" not in job.command,
                    f"Job parametrization config issue: job name [{job.name}], job command: [{job.command}]",
                    workflow.name,
                )

            cls.validate_file_paths_in_run_command(workflow)
            cls.validate_file_paths_in_digest_configs(workflow)
            cls.validate_requirements_txt_files(workflow)
            cls.validate_dockers(workflow)

            if workflow.event == Workflow.Event.SCHEDULE:
                cls.evaluate_check(
                    workflow.cron_schedules
                    and isinstance(workflow.cron_schedules, list),
                    f".crone_schedules str must be non-empty list of cron strings .event===SCHEDULE, provided value [{workflow.cron_schedules}]",
                    workflow.name,
                )

                def is_valid_cron_field(field: str) -> bool:
                    """Check if a cron field is valid (supports *, digits, ranges, steps, and lists)"""
                    if field == "*":
                        return True
                    # Check for step values like */5 or 1-10/2
                    if "/" in field:
                        base, step = field.split("/", 1)
                        if not step.isdigit():
                            return False
                        if base != "*":
                            field = base  # Continue validating the base part
                        else:
                            return True  # */N is valid
                    # Check for lists like 1,3,5 or ranges like 1-5
                    for part in field.split(","):
                        if "-" in part:
                            # Range like 1-5
                            parts = part.split("-")
                            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                                return False
                        elif not part.isdigit():
                            return False
                    return True

                for cron_schedule in workflow.cron_schedules:
                    cls.evaluate_check(
                        len(cron_schedule.split(" ")) == 5,
                        f".crone_schedules must be posix compliant cron str, e.g. '30 15 * * *', provided value [{cron_schedule}]",
                        workflow.name,
                    )
                    tokens = cron_schedule.split(" ")
                    for i, cron_token in enumerate(tokens):
                        field_name = ["minute", "hour", "day", "month", "day_of_week"][
                            i
                        ]
                        cls.evaluate_check(
                            is_valid_cron_field(cron_token),
                            f".crone_schedules must be posix compliant cron str, e.g. '30 15 * * 1-5', provided value [{cron_schedule}], invalid {field_name} field [{cron_token}]",
                            workflow.name,
                        )

            if workflow.artifacts:
                for artifact in workflow.artifacts:
                    cls.evaluate_check(
                        isinstance(artifact, Artifact.Config),
                        f"Must be Artifact.Config type, not {type(artifact)}: [{artifact}]",
                        workflow.name,
                    )
                    if artifact.is_s3_artifact():
                        assert (
                            Settings.S3_ARTIFACT_PATH
                        ), "Provide S3_ARTIFACT_PATH setting in any .py file in ./ci/settings/* to be able to use s3 for artifacts"

            for job in workflow.jobs:
                if job.requires and workflow.artifacts:
                    for require in job.requires:
                        if (
                            require in workflow.artifacts
                            and workflow.artifacts[require].is_s3_artifact()
                        ):
                            assert not any(
                                [r in GHRunners for r in job.runs_on]
                            ), f"GH runners [{job.name}:{job.runs_on}] must not be used with S3 as artifact storage"

            if workflow.enable_cache:
                assert (
                    Settings.CI_CONFIG_RUNS_ON
                ), f"Runner label to run workflow config job must be provided via CACHE_CONFIG_RUNS_ON setting if enable_cache=True, workflow [{workflow.name}]"

                assert (
                    Settings.CACHE_S3_PATH
                ), f"CACHE_S3_PATH Setting must be defined if enable_cache=True, workflow [{workflow.name}]"

            if workflow.dockers:
                if Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB == False:
                    cls.evaluate_check_simple(
                        Settings.DOCKER_BUILD_ARM_RUNS_ON
                        and Settings.DOCKER_MERGE_RUNS_ON
                        and Settings.DOCKER_BUILD_AMD_RUNS_ON
                        and Settings.DOCKER_BUILD_ARM_RUNS_ON
                        != Settings.DOCKER_BUILD_AMD_RUNS_ON,
                        f"Settings: DOCKER_MERGE_RUNS_ON, DOCKER_BUILD_ARM_RUNS_ON, DOCKER_BUILD_AMD_RUNS_ON must be provided and be different CPU architecture machines",
                    )
                else:
                    cls.evaluate_check(
                        Settings.DOCKER_MERGE_RUNS_ON,
                        f"DOCKER_BUILD_AND_MERGE_RUNS_ON settings must be defined if workflow has dockers",
                        workflow_name=workflow.name,
                    )

            if workflow.set_latest_for_docker_merged_manifest:
                cls.evaluate_check(
                    workflow.enable_dockers_manifest_merge,
                    f".set_latest_for_docker_merged_manifest workflow setting is applicable with .enable_dockers_manifest_merge=True",
                    workflow_name=workflow.name,
                )

            if workflow.enable_report:
                assert (
                    Settings.HTML_S3_PATH
                ), f"HTML_S3_PATH Setting must be defined if enable_html=True, workflow [{workflow.name}]"
                assert (
                    Settings.S3_BUCKET_TO_HTTP_ENDPOINT
                ), f"S3_BUCKET_TO_HTTP_ENDPOINT Setting must be defined if enable_html=True, workflow [{workflow.name}]"
                assert (
                    Settings.HTML_S3_PATH.split("/")[0]
                    in Settings.S3_BUCKET_TO_HTTP_ENDPOINT
                ), f"S3_BUCKET_TO_HTTP_ENDPOINT Setting must include bucket name [{Settings.HTML_S3_PATH}] from HTML_S3_PATH, workflow [{workflow.name}]"

            if workflow.enable_cache:
                for artifact in workflow.artifacts or []:
                    assert (
                        artifact.is_s3_artifact()
                    ), f"All artifacts must be of S3 type if enable_cache|enable_html=True, artifact [{artifact.name}], type [{artifact.type}], workflow [{workflow.name}]"

            if workflow.dockers and not workflow.disable_dockers_build:
                assert (
                    Settings.DOCKERHUB_USERNAME
                ), f"Settings.DOCKERHUB_USERNAME must be provided if workflow has dockers, workflow [{workflow.name}]"
                assert (
                    Settings.DOCKERHUB_SECRET
                ), f"Settings.DOCKERHUB_SECRET must be provided if workflow has dockers, workflow [{workflow.name}]"
                assert workflow.get_secret(
                    Settings.DOCKERHUB_SECRET
                ), f"Secret [{Settings.DOCKERHUB_SECRET}] must have configuration in workflow.secrets, workflow [{workflow.name}]"

            if workflow.enable_flaky_tests_catalog:
                cls.evaluate_check(
                    workflow.enable_merge_ready_status,
                    f".enable_flaky_tests_catalog workflow setting is applicable with .enable_merge_ready_status=True",
                    workflow_name=workflow.name,
                )

            if (
                workflow.enable_cache
                or workflow.enable_report
                or workflow.enable_merge_ready_status
            ):
                for job in workflow.jobs:
                    assert not any(
                        job in ("ubuntu-latest",) for job in job.runs_on
                    ), f"GitHub Runners must not be used for workflow with enabled: workflow.enable_cache, workflow.enable_html or workflow.enable_merge_ready_status as s3 access is required, workflow [{workflow.name}], job [{job.name}]"

            if workflow.enable_cidb:
                cls.evaluate_check(
                    Settings.SECRET_CI_DB_URL,
                    "Settings.SECRET_CI_DB_URL must be provided if workflow.enable_cidb=True",
                    workflow,
                )
                cls.evaluate_check(
                    Settings.SECRET_CI_DB_USER,
                    "Settings.SECRET_CI_DB_USER must be provided if workflow.enable_cidb=True",
                    workflow,
                )
                cls.evaluate_check(
                    Settings.SECRET_CI_DB_PASSWORD,
                    "Settings.SECRET_CI_DB_PASSWORD must be provided if workflow.enable_cidb=True",
                    workflow,
                )
                cls.evaluate_check(
                    Settings.CI_DB_DB_NAME,
                    "Settings.CI_DB_DB_NAME must be provided if workflow.enable_cidb=True",
                    workflow,
                )
                cls.evaluate_check(
                    Settings.CI_DB_TABLE_NAME,
                    "Settings.CI_DB_TABLE_NAME must be provided if workflow.enable_cidb=True",
                    workflow,
                )

            if workflow.enable_gh_summary_comment:
                cls.evaluate_check(
                    workflow.event == Workflow.Event.PULL_REQUEST,
                    ".enable_gh_summary_comment=True applicable for pull_request workflow only",
                    workflow,
                )
                cls.evaluate_check(
                    workflow.enable_report,
                    ".enable_gh_summary_comment=True requires .enable_report==True",
                    workflow,
                )

    @classmethod
    def validate_file_paths_in_run_command(cls, workflow: Workflow.Config) -> None:
        if not Settings.VALIDATE_FILE_PATHS:
            return
        for job in workflow.jobs:
            run_command = job.command
            command_parts = run_command.split(" ")
            for part in command_parts:
                if ">" in part:
                    return
                if "/" in part:
                    assert (
                        Path(part).is_file() or Path(part).is_dir()
                    ), f"Apparently run command [{run_command}] for job [{job}] has invalid path [{part}]. Setting to disable check: VALIDATE_FILE_PATHS"
                    break

    @classmethod
    def validate_file_paths_in_digest_configs(cls, workflow: Workflow.Config) -> None:
        if not Settings.VALIDATE_FILE_PATHS:
            return
        for job in workflow.jobs:
            if not job.digest_config:
                continue
            for include_path in chain(
                job.digest_config.include_paths, job.digest_config.exclude_paths
            ):
                if "*" in include_path:
                    assert glob.glob(
                        include_path, recursive=True
                    ), f"Apparently file glob [{include_path}] in job [{job.name}] digest_config [{job.digest_config}] invalid, workflow [{workflow.name}]. Setting to disable check: VALIDATE_FILE_PATHS"
                else:
                    assert (
                        Path(include_path).is_file() or Path(include_path).is_dir()
                    ), f"Invalid file path [{include_path}] in job [{job.name}] digest_config, workflow [{workflow.name}]. Setting to disable check: VALIDATE_FILE_PATHS"

    @classmethod
    def validate_requirements_txt_files(cls, workflow: Workflow.Config) -> None:
        for job in workflow.jobs:
            if job.job_requirements:
                if job.job_requirements.python_requirements_txt:
                    path = Path(job.job_requirements.python_requirements_txt)
                    message = f"File with py requirement [{path}] does not exist"
                    if job.name in (
                        Settings.DOCKER_BUILD_AMD_LINUX_AND_MERGE_JOB_NAME,
                        Settings.CI_CONFIG_JOB_NAME,
                        Settings.FINISH_WORKFLOW_JOB_NAME,
                    ):
                        message += '\n  If all requirements already installed on your runners - add setting INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS""'
                        message += "\n  If requirements needs to be installed - add requirements file (Settings.INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS):"
                        message += "\n      echo jwt==1.3.1 > ./ci/requirements.txt"
                        message += (
                            "\n      echo requests==2.32.4 >> ./ci/requirements.txt"
                        )
                        message += "\n      echo https://clickhouse-builds.s3.amazonaws.com/packages/praktika-0.1-py3-none-any.whl >> ./ci/requirements.txt"
                    cls.evaluate_check(path.is_file(), message, job.name, workflow.name)

    @classmethod
    def validate_dockers(cls, workflow: Workflow.Config):
        names = []
        for docker in workflow.dockers:
            cls.evaluate_check(
                docker.name not in names,
                f"Non uniq docker name [{docker.name}]",
                workflow_name=workflow.name,
            )
            names.append(docker.name)
        for docker in workflow.dockers:
            for docker_dep in docker.depends_on:
                cls.evaluate_check(
                    docker_dep in names,
                    f"Docker [{docker.name}] has invalid dependency [{docker_dep}]",
                    workflow_name=workflow.name,
                )

    @classmethod
    def evaluate_check(cls, check_ok, message, workflow_name, job_name=""):
        message = message.split("\n")
        messages = [message] if not isinstance(message, list) else message
        if check_ok:
            return
        else:
            print(
                f"ERROR: Config validation failed: workflow [{workflow_name}], job [{job_name}]:"
            )
            for message in messages:
                print(" ||  " + message)
            raise

    @classmethod
    def evaluate_check_simple(cls, check_ok, message):
        message = message.split("\n")
        messages = [message] if not isinstance(message, list) else message
        if check_ok:
            return
        else:
            print(f"ERROR: Validation failed:")
            for message in messages:
                print(" ||  " + message)
            raise
