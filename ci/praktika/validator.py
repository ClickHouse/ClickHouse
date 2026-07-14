import glob
import re
import sys
from itertools import chain
from pathlib import Path

from praktika import Artifact, Job

from . import Workflow
from .mangle import _get_workflows
from .settings import GHRunners, Settings


class Validator:
    @classmethod
    def _s3_bucket_name(cls, value: str) -> str:
        return str(value or "").removeprefix("s3://").split("/", maxsplit=1)[0]

    @classmethod
    def validate_infrastructure_deploy(cls, cloud):
        print("---Start validating Infrastructure and settings---")

        # Intra-slug separator is "_" (matches CloudInfrastructure._project_prefix).
        # Names are normalized the same way, so the prefix boundary is "_" too.
        project_prefix = re.sub(
            r"_{2,}",
            "_",
            re.sub(r"[^a-z0-9]+", "_", (cloud.name or "").lower()),
        ).strip("_")
        for group, names in getattr(cloud, "_pre_namespace_names", {}).items():
            for name in names:
                normalized = re.sub(
                    r"_{2,}",
                    "_",
                    re.sub(r"[^a-z0-9]+", "_", str(name).lstrip("/").lower()),
                ).strip("_")
                cls.evaluate_check_simple(
                    not project_prefix
                    or (
                        normalized != project_prefix
                        and not normalized.startswith(f"{project_prefix}_")
                    ),
                    f"Infrastructure {group} item name [{name}] already includes "
                    f"project prefix [{project_prefix}]. Use project-local names; "
                    "CloudInfrastructure.Config adds the project prefix automatically.",
                )

        storage_names = {storage.name for storage in getattr(cloud, "storages", [])}

        def _check_setting_bucket(setting_name: str, setting_value: str):
            bucket = cls._s3_bucket_name(setting_value)
            if not bucket:
                return ""
            cls.evaluate_check_simple(
                not storage_names or bucket in storage_names,
                f"Setting {setting_name} bucket [{bucket}] must match one of "
                f"infrastructure Storage names [{', '.join(sorted(storage_names))}]",
            )
            return bucket

        referenced_buckets = {
            bucket
            for bucket in (
                _check_setting_bucket("S3_ARTIFACT_BUCKET", Settings.S3_ARTIFACT_BUCKET),
                _check_setting_bucket("S3_REPORT_BUCKET", Settings.S3_REPORT_BUCKET),
                _check_setting_bucket("CACHE_S3_PATH", Settings.CACHE_S3_PATH),
            )
            if bucket
        }

        for report_page in getattr(cloud, "report_pages", []) or []:
            bucket = cls._s3_bucket_name(
                getattr(report_page, "bucket_name", "") or Settings.S3_REPORT_BUCKET
            )
            if not bucket:
                continue
            referenced_buckets.add(bucket)
            cls.evaluate_check_simple(
                not storage_names or bucket in storage_names,
                f"ReportPage bucket [{bucket}] must match one of infrastructure "
                f"Storage names [{', '.join(sorted(storage_names))}]",
            )

        endpoint_map = Settings.S3_BUCKET_TO_HTTP_ENDPOINT or {}
        for bucket in sorted(referenced_buckets):
            cls.evaluate_check_simple(
                bucket in endpoint_map,
                f"S3_BUCKET_TO_HTTP_ENDPOINT must include bucket [{bucket}] used by "
                "infrastructure/settings S3 configuration",
            )

        image_builders = getattr(cloud, "image_builders", []) or []
        if Settings.PRAKTIKA_BASE_VENV and image_builders:
            venv_names = {
                venv.name
                for builder in image_builders
                for venv in getattr(builder, "prebuilt_venvs", []) or []
                if getattr(venv, "name", "")
            }
            expected = Settings.PRAKTIKA_BASE_VENV
            cls.evaluate_check_simple(
                expected in venv_names,
                f"Setting PRAKTIKA_BASE_VENV [{expected}] must match one of "
                f"ImageBuilder prebuilt venv names [{', '.join(sorted(venv_names))}]",
            )

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
                bool(Settings.SECRET_GH_APP or Settings.GH_AUTH_LAMBDA_NAME),
                "Setting SECRET_GH_APP or GH_AUTH_LAMBDA_NAME must be provided with USE_CUSTOM_GH_AUTH == True",
            )

        # NOTE: disabled — this is deploy-time validation (infra project-name
        # uniqueness) and requires ./ci/infrastructure/projects.py to exist.
        # Pipeline/settings validation also runs on runners, whose checkout may
        # not ship the infra config, so it wrongly failed with "Infrastructure
        # config file does not exist". Re-enable behind a deploy-only guard.
        # if Settings.CLOUD_INFRASTRUCTURE_CONFIG_PATH:
        #     projects = _get_infra_projects()
        #     normalized = {}
        #     for project in projects:
        #         normalized_name = re.sub(
        #             r"_{2,}",
        #             "_",
        #             re.sub(r"[^a-z0-9]+", "_", project.name.lower()),
        #         ).strip("_")
        #         cls.evaluate_check_simple(
        #             normalized_name,
        #             f"Infrastructure project name [{project.name}] must normalize to a non-empty AWS-safe prefix",
        #         )
        #         cls.evaluate_check_simple(
        #             normalized_name not in normalized,
        #             f"Infrastructure project names [{normalized.get(normalized_name)}] and [{project.name}] normalize to the same prefix [{normalized_name}]",
        #         )
        #         normalized[normalized_name] = project.name

        _VALID_ENGINES = (Workflow.Engine.PRAKTIKA, Workflow.Engine.GH_ACTIONS)
        files = []
        workflows = _get_workflows(_for_validation_check=True, _file_names_out=files)
        from collections import Counter
        file_counts = Counter(files)
        for file, count in file_counts.items():
            cls.evaluate_check_simple(
                count == 1,
                f"Workflow file [{file}] must define exactly one workflow in WORKFLOWS (found {count})",
            )
        for workflow in workflows:
            print(f"Validating workflow [{workflow.name}]")
            cls.evaluate_check(
                workflow.engine in _VALID_ENGINES,
                f"Invalid engine [{workflow.engine}], must be one of {_VALID_ENGINES}",
                workflow.name,
            )
            # NOTE: disabled — like job.enable_commit_status, the workflow-level
            # enable_commit_status_on_failure is harmless on the Praktika engine
            # (the Checks API is used regardless), so don't fail validation when
            # a workflow carries the flag.
            # if workflow.engine == Workflow.Engine.PRAKTIKA:
            #     cls.evaluate_check(
            #         not workflow.enable_commit_status_on_failure,
            #         ".enable_commit_status_on_failure is redundant for Praktika engine workflows: the GitHub Checks API is used and always publishes workflow/job check status",
            #         workflow.name,
            #     )
            if Settings.USE_CUSTOM_GH_AUTH and workflow.enable_report:
                if not Settings.GH_AUTH_LAMBDA_NAME:
                    secret = workflow.get_secret(Settings.SECRET_GH_APP)
                    cls.evaluate_check(
                        bool(secret),
                        f"Secret [{Settings.SECRET_GH_APP}] must be configured for workflow",
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
                if workflow.engine != Workflow.Engine.GH_ACTIONS:
                    # "self-hosted" is a GitHub-Actions runner-group label with
                    # no meaning to the praktika engine (which routes by the
                    # pool/size label); ignore it when counting.
                    effective_runs_on = [
                        label for label in (job.runs_on or []) if label != "self-hosted"
                    ]
                    cls.evaluate_check(
                        len(effective_runs_on) == 1,
                        f"Non-GHActions workflow jobs must have exactly one runs_on "
                        f"label (excluding 'self-hosted'), got [{job.runs_on}] for [{job.name}]",
                        workflow.name,
                    )
                # NOTE: disabled — `enable_commit_status` is harmless on the
                # Praktika engine (it just uses the Checks API regardless), so
                # don't fail validation when a job carries the flag.
                # if workflow.engine == Workflow.Engine.PRAKTIKA:
                #     cls.evaluate_check(
                #         not job.enable_commit_status,
                #         ".enable_commit_status is redundant for Praktika engine workflows: the GitHub Checks API is used and always publishes workflow/job check status",
                #         workflow.name,
                #         job.name,
                #     )
                cls.evaluate_check(
                    "PARAMETER" not in job.command,
                    f"Job parametrization config issue: job name [{job.name}], job command: [{job.command}]",
                    workflow.name,
                )

            cls.validate_file_paths_in_run_command(workflow)
            cls.validate_file_paths_in_digest_configs(workflow)
            cls.validate_dockers(workflow)
            cls.validate_job_names(workflow)

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
                            Settings.S3_ARTIFACT_BUCKET
                        ), "Provide S3_ARTIFACT_BUCKET setting in any .py file in ./ci/settings/* to be able to use s3 for artifacts"

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
                if not Settings.ENABLE_MULTIPLATFORM_DOCKER_IN_ONE_JOB:
                    cls.evaluate_check_simple(
                        Settings.DOCKER_BUILD_ARM_RUNS_ON
                        and Settings.DOCKER_MERGE_RUNS_ON
                        and Settings.DOCKER_BUILD_AMD_RUNS_ON
                        and Settings.DOCKER_BUILD_ARM_RUNS_ON
                        != Settings.DOCKER_BUILD_AMD_RUNS_ON,
                        "Settings: DOCKER_MERGE_RUNS_ON, DOCKER_BUILD_ARM_RUNS_ON, DOCKER_BUILD_AMD_RUNS_ON must be provided and be different CPU architecture machines",
                    )
                else:
                    cls.evaluate_check(
                        Settings.DOCKER_MERGE_RUNS_ON,
                        "DOCKER_BUILD_AND_MERGE_RUNS_ON settings must be defined if workflow has dockers",
                        workflow_name=workflow.name,
                    )

            if workflow.set_latest_for_docker_merged_manifest:
                cls.evaluate_check(
                    workflow.enable_dockers_manifest_merge,
                    ".set_latest_for_docker_merged_manifest workflow setting is applicable with .enable_dockers_manifest_merge=True",
                    workflow_name=workflow.name,
                )

            if workflow.enable_open_issues_check:
                cls.evaluate_check(
                    workflow.enable_report,
                    ".enable_open_issues_check workflow setting is applicable with .enable_report=True",
                    workflow_name=workflow.name,
                )

            if workflow.enable_report:
                assert (
                    Settings.S3_REPORT_BUCKET
                ), f"S3_REPORT_BUCKET Setting must be defined if enable_html=True, workflow [{workflow.name}]"
                assert (
                    Settings.S3_BUCKET_TO_HTTP_ENDPOINT
                ), f"S3_BUCKET_TO_HTTP_ENDPOINT Setting must be defined if enable_html=True, workflow [{workflow.name}]"
                assert (
                    Settings.S3_REPORT_BUCKET.split("/")[0]
                    in Settings.S3_BUCKET_TO_HTTP_ENDPOINT
                ), f"S3_BUCKET_TO_HTTP_ENDPOINT Setting must include bucket name [{Settings.S3_REPORT_BUCKET}] from S3_REPORT_BUCKET, workflow [{workflow.name}]"

            if workflow.enable_cache:
                for artifact in workflow.artifacts or []:
                    assert (
                        artifact.is_s3_artifact()
                    ), f"All artifacts must be of S3 type if enable_cache|enable_html=True, artifact [{artifact.name}], type [{artifact.type}], workflow [{workflow.name}]"

            if workflow.dockers and not workflow.disable_dockers_build:
                assert Settings.SECRET_DOCKER_REGISTRY, (
                    f"Settings.SECRET_DOCKER_REGISTRY must be set when the workflow "
                    f"manages docker images (.dockers is set and "
                    f".disable_dockers_build is False): praktika logs in to the "
                    f"registry to build/push them. Point it at a secret whose value "
                    f'is {{"username": ..., "password": ...}}. Workflow [{workflow.name}]'
                )
                assert workflow.get_secret(Settings.SECRET_DOCKER_REGISTRY), (
                    f"Docker registry secret [{Settings.SECRET_DOCKER_REGISTRY}] "
                    f"(Settings.SECRET_DOCKER_REGISTRY) is not registered in the "
                    f"workflow's secrets. Add it to the project SECRETS so the "
                    f"workflow can resolve it. Workflow [{workflow.name}]"
                )

            if workflow.enable_open_issues_check:
                cls.evaluate_check(
                    workflow.enable_merge_ready_status,
                    ".enable_open_issues_check workflow setting is applicable with .enable_merge_ready_status=True",
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
                    Settings.SECRET_CI_DB_CONNECTION,
                    "Settings.SECRET_CI_DB_CONNECTION must be provided if workflow.enable_cidb=True",
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
    def validate_job_names(cls, workflow: Workflow.Config):
        names_lower = {}
        for job in workflow.jobs:
            job_name_lower = job.name.lower()
            if job_name_lower in names_lower:
                cls.evaluate_check(
                    False,
                    f"Duplicate job name (case-insensitive): [{job.name}] conflicts with [{names_lower[job_name_lower]}]",
                    workflow_name=workflow.name,
                )
            names_lower[job_name_lower] = job.name

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
            sys.exit(1)

    @classmethod
    def evaluate_check_simple(cls, check_ok, message):
        message = message.split("\n")
        messages = [message] if not isinstance(message, list) else message
        if check_ok:
            return
        else:
            print("ERROR: Validation failed:")
            for message in messages:
                print(" ||  " + message)
            sys.exit(1)
