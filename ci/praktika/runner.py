import glob
import json
import os
import re
import sys
import traceback
from pathlib import Path

from ._environment import _Environment
from .artifact import Artifact
from .cidb import CIDB
from .digest import Digest
from .gh import GH
from .hook_cache import CacheRunnerHooks
from .hook_html import HtmlRunnerHooks
from .info import Info
from .native_jobs import _is_praktika_job
from .result import Result, ResultInfo
from .runtime import RunConfig
from .s3 import S3
from .settings import Settings
from .usage import ComputeUsage, StorageUsage
from .utils import Shell, TeePopen, Utils

_GH_authenticated = False


def _GH_Auth(workflow):
    global _GH_authenticated
    if _GH_authenticated:
        return
    if not Settings.USE_CUSTOM_GH_AUTH:
        return
    from .gh_auth import GHAuth

    if not Shell.check(f"gh auth status", verbose=True):
        pem = workflow.get_secret(Settings.SECRET_GH_APP_PEM_KEY).get_value()
        app_id = workflow.get_secret(Settings.SECRET_GH_APP_ID).get_value()
        GHAuth.auth(app_id=app_id, app_key=pem)
    _GH_authenticated = True


class Runner:
    @staticmethod
    def generate_local_run_environment(workflow, job, pr=None, sha=None, branch=None):
        print("WARNING: Generate dummy env for local test")
        Shell.check(f"mkdir -p {Settings.TEMP_DIR}", strict=True)
        os.environ["JOB_NAME"] = job.name
        os.environ["CHECK_NAME"] = job.name
        assert (bool(pr) ^ bool(branch)) or (not pr and not branch)
        pr = pr or -1
        if branch:
            pr = 0
        _Environment(
            WORKFLOW_NAME=workflow.name,
            JOB_NAME=job.name,
            REPOSITORY="",
            BRANCH=branch,
            SHA=sha or Shell.get_output("git rev-parse HEAD"),
            PR_NUMBER=pr if not branch else 0,
            EVENT_TYPE=workflow.event,
            JOB_OUTPUT_STREAM="",
            EVENT_FILE_PATH="",
            CHANGE_URL="",
            COMMIT_URL="",
            BASE_BRANCH="",
            RUN_URL="",
            RUN_ID="",
            INSTANCE_ID="",
            INSTANCE_TYPE="",
            INSTANCE_LIFE_CYCLE="",
            LOCAL_RUN=True,
            PR_BODY="",
            PR_TITLE="",
            USER_LOGIN="",
            FORK_NAME="",
            PR_LABELS=[],
        ).dump()
        workflow_config = RunConfig(
            name=workflow.name,
            digest_jobs={},
            digest_dockers={},
            sha="",
            cache_success=[],
            cache_success_base64=[],
            cache_artifacts={},
            cache_jobs={},
            filtered_jobs={},
            custom_data={},
        )
        for docker in workflow.dockers:
            workflow_config.digest_dockers[docker.name] = Digest().calc_docker_digest(
                docker, workflow.dockers
            )

        workflow_config.dump()

        Result.create_from(name=job.name, status=Result.Status.PENDING).dump()

    def _setup_env(self, _workflow, job):
        # source env file to write data into fs (workflow config json, workflow status json)
        Shell.check(f". {Settings.ENV_SETUP_SCRIPT}", verbose=True, strict=True)

        # parse the same env script and apply envs from python so that this process sees them
        with open(Settings.ENV_SETUP_SCRIPT, "r") as f:
            content = f.read()
        export_pattern = re.compile(
            r"export (\w+)=\$\(cat<<\'EOF\'\n(.*?)EOF\n\)", re.DOTALL
        )
        matches = export_pattern.findall(content)
        for key, value in matches:
            value = value.strip()
            os.environ[key] = value
            print(f"Set environment variable {key}.")

        print("Read GH Environment")
        env = _Environment.from_env()
        env.JOB_NAME = job.name
        os.environ["JOB_NAME"] = job.name
        os.environ["CHECK_NAME"] = job.name
        env.JOB_CONFIG = job
        env.dump()
        print(env)

        return 0

    def _pre_run(self, workflow, job, local_run=False):
        if job.name == Settings.CI_CONFIG_JOB_NAME:
            GH.print_actions_debug_info()
        env = _Environment.get()

        result = Result(
            name=job.name,
            status=Result.Status.RUNNING,
            start_time=Utils.timestamp(),
        )
        result.dump()

        if not local_run:
            if workflow.enable_report and job.name != Settings.CI_CONFIG_JOB_NAME:
                print("Update Job and Workflow Report")
                HtmlRunnerHooks.pre_run(workflow, job)

        if job.requires and not _is_praktika_job(job.name):
            print("Download required artifacts")
            required_artifacts = []
            # praktika service jobs do not require any of artifacts and excluded in if to not upload "hacky" artifact report.
            #  this artifact is created to replace legacy build_report and maintain seamless transition to praktika
            #  once there is no need for this "hacky" artifact report - second condition in "if" can be removed
            for requires_artifact_name in job.requires:
                for artifact in workflow.artifacts:
                    if (
                        artifact.name == requires_artifact_name
                        and artifact.type == Artifact.Type.S3
                    ):
                        required_artifacts.append(artifact)
                else:
                    if (
                        requires_artifact_name
                        in [job.name for job in workflow.jobs if job.provides]
                        and Settings.ENABLE_ARTIFACTS_REPORT
                    ):
                        print(
                            f"Artifact report for [{requires_artifact_name}] will be uploaded"
                        )
                        required_artifacts.append(
                            Artifact.Config(
                                name=requires_artifact_name,
                                type=Artifact.Type.PHONY,
                                path=f"artifact_report_{Utils.normalize_string(requires_artifact_name)}.json",
                                _provided_by=requires_artifact_name,
                            )
                        )

            print(f"--- Job requires s3 artifacts [{required_artifacts}]")
            if workflow.enable_cache:
                prefixes = CacheRunnerHooks.pre_run(
                    _job=job, _workflow=workflow, _required_artifacts=required_artifacts
                )
            else:
                prefixes = [env.get_s3_prefix()] * len(required_artifacts)
            for artifact, prefix in zip(required_artifacts, prefixes):
                if artifact.compress_zst:
                    assert not isinstance(
                        artifact.path, (tuple, list)
                    ), "Not yes supported for compressed artifacts"
                    artifact.path = f"{Path(artifact.path).name}.zst"

                if isinstance(artifact.path, (tuple, list)):
                    artifact_paths = artifact.path
                else:
                    artifact_paths = [artifact.path]

                for artifact_path in artifact_paths:
                    recursive = False
                    include_pattern = ""
                    if "*" in artifact_path:
                        s3_path = f"{Settings.S3_ARTIFACT_PATH}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/"
                        recursive = True
                        include_pattern = Path(artifact_path).name
                        assert "*" in include_pattern
                    else:
                        s3_path = f"{Settings.S3_ARTIFACT_PATH}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/{Path(artifact_path).name}"
                    S3.copy_file_from_s3(
                        s3_path=s3_path,
                        local_path=Settings.INPUT_DIR,
                        recursive=recursive,
                        include_pattern=include_pattern,
                    )

                    if artifact.compress_zst:
                        Utils.decompress_file(Path(Settings.INPUT_DIR) / artifact_path)

        return 0

    def _run(self, workflow, job, docker="", no_docker=False, param=None, test=""):
        # re-set envs for local run
        env = _Environment.get()
        env.JOB_NAME = job.name
        env.dump()

        # work around for old clickhouse jobs
        os.environ["PRAKTIKA"] = "1"
        if job.name != Settings.CI_CONFIG_JOB_NAME:
            try:
                os.environ["DOCKER_TAG"] = json.dumps(
                    RunConfig.from_fs(workflow.name).digest_dockers
                )
            except Exception as e:
                traceback.print_exc()
                print(f"WARNING: Failed to set DOCKER_TAG, ex [{e}]")

        if param:
            if not isinstance(param, str):
                Utils.raise_with_error(
                    f"Custom param for local tests must be of type str, got [{type(param)}]"
                )

        if job.run_in_docker and not no_docker:
            job.run_in_docker, docker_settings = (
                job.run_in_docker.split("+")[0],
                job.run_in_docker.split("+")[1:],
            )
            from_root = "root" in docker_settings
            settings = [s for s in docker_settings if s.startswith("-")]
            if ":" in job.run_in_docker:
                docker_name, docker_tag = job.run_in_docker.split(":")
                print(
                    f"WARNING: Job [{job.name}] use custom docker image with a tag - praktika won't control docker version"
                )
            else:
                docker_name, docker_tag = (
                    job.run_in_docker,
                    RunConfig.from_fs(workflow.name).digest_dockers[job.run_in_docker],
                )
                if Utils.is_arm():
                    docker_tag += "_arm"
                elif Utils.is_amd():
                    docker_tag += "_amd"
                else:
                    raise RuntimeError("Unsupported CPU architecture")

            docker = docker or f"{docker_name}:{docker_tag}"
            current_dir = os.getcwd()
            for setting in settings:
                if setting.startswith("--volume"):
                    volume = setting.removeprefix("--volume=").split(":")[0]
                    if not Path(volume).exists():
                        print(
                            "WARNING: Create mount dir point in advance to have the same owner"
                        )
                        Shell.check(f"mkdir -p {volume}", verbose=True, strict=True)
            Shell.check(
                "docker ps -a --format '{{.Names}}' | grep -q praktika && docker rm -f praktika",
                verbose=True,
            )
            cmd = f"docker run --rm --name praktika {'--user $(id -u):$(id -g)' if not from_root else ''} -e PYTHONPATH='.:./ci' --volume ./:{current_dir} --workdir={current_dir} {' '.join(settings)} {docker} {job.command}"
        else:
            cmd = job.command
            python_path = os.getenv("PYTHONPATH", ":")
            os.environ["PYTHONPATH"] = f".:{python_path}"

        if param:
            print(f"Custom --param [{param}] will be passed to job's script")
            cmd += f" --param {param}"
        if test:
            print(f"Custom --test [{test}] will be passed to job's script")
            cmd += f" --test {test}"
        print(f"--- Run command [{cmd}]")

        with TeePopen(cmd, timeout=job.timeout) as process:
            start_time = Utils.timestamp()
            if Path((Result.experimental_file_name_static())).exists():
                # experimental mode to let job write results into fixed result.json file instead of result_job_name.json
                Path(Result.experimental_file_name_static()).unlink()

            exit_code = process.wait()

            if Path(Result.experimental_file_name_static()).exists():
                result = Result.experimental_from_fs(job.name)
                if not result.start_time:
                    print(
                        "WARNING: no start_time set by the job - set job start_time/duration"
                    )
                    result.start_time = start_time
                    result.dump()

            result = Result.from_fs(job.name)
            if exit_code != 0:
                if not result.is_completed():
                    if process.timeout_exceeded:
                        print(
                            f"WARNING: Job timed out: [{job.name}], timeout [{job.timeout}], exit code [{exit_code}]"
                        )
                        info = ResultInfo.TIMEOUT
                    elif result.is_running():
                        info = f"ERROR: Job killed, exit code [{exit_code}]  - set status to [{Result.Status.ERROR}]."
                        print(info)
                    else:
                        info = f"ERROR: Invalid status [{result.status}] for exit code [{exit_code}]  - switch to [{Result.Status.ERROR}]"
                        print(info)
                    result.set_status(Result.Status.ERROR)
                    result.set_info(info)
                    result.set_info("---").set_info(
                        process.get_latest_log(max_lines=20)
                    ).set_info("---")
            result.dump()

        return exit_code

    def _post_run(
        self, workflow, job, setup_env_exit_code, prerun_exit_code, run_exit_code
    ):
        info_errors = []
        env = _Environment.get()
        result_exist = Result.exist(job.name)
        is_ok = True

        if setup_env_exit_code != 0:
            info = f"ERROR: {ResultInfo.SETUP_ENV_JOB_FAILED}"
            print(info)
            # set Result with error and logs
            Result(
                name=job.name,
                status=Result.Status.ERROR,
                start_time=Utils.timestamp(),
                duration=0.0,
                info=info,
            ).dump()
        elif prerun_exit_code != 0:
            info = ResultInfo.PRE_JOB_FAILED
            print(info)
            # set Result with error and logs
            Result(
                name=job.name,
                status=Result.Status.ERROR,
                start_time=Utils.timestamp(),
                duration=0.0,
                info=info,
            ).dump()
        elif not result_exist:
            info = f"ERROR: {ResultInfo.NOT_FOUND_IMPOSSIBLE}"
            print(info)
            Result(
                name=job.name,
                start_time=Utils.timestamp(),
                duration=None,
                status=Result.Status.ERROR,
                info=ResultInfo.NOT_FOUND_IMPOSSIBLE,
            ).dump()

        try:
            result = Result.from_fs(job.name)
        except Exception as e:  # json.decoder.JSONDecodeError
            print(f"ERROR: Failed to read Result json from fs, ex: [{e}]")
            traceback.print_exc()
            result = Result.create_from(
                status=Result.Status.ERROR,
                info=f"Failed to read Result json, ex: [{e}]",
            ).dump()

        if not result.is_completed():
            info = f"ERROR: {ResultInfo.KILLED}"
            print(info)
            result.set_info(info).set_status(Result.Status.ERROR).dump()
        elif (
            not result.is_ok()
            and workflow.enable_merge_ready_status
            and not job.allow_merge_on_failure
        ):
            print("set required label")
            result.set_required_label()

        result.update_duration()
        # if result.is_error():
        result.set_files([Settings.RUN_LOG])

        if job.post_hooks:
            sw_ = Utils.Stopwatch()
            results_ = []
            for check in job.post_hooks:
                if callable(check):
                    name = check.__name__
                else:
                    name = str(check)
                results_.append(Result.from_commands_run(name=name, command=check))
            result.results.append(
                Result.create_from(name="Post Hooks", results=results_, stopwatch=sw_)
            )

        if run_exit_code == 0:
            providing_artifacts = []
            if job.provides and workflow.artifacts:
                for provides_artifact_name in job.provides:
                    for artifact in workflow.artifacts:
                        if (
                            artifact.name == provides_artifact_name
                            and artifact.type == Artifact.Type.S3
                        ):
                            providing_artifacts.append(artifact)
            if providing_artifacts:
                print(f"Job provides s3 artifacts [{providing_artifacts}]")
                artifact_links = []
                s3_path = f"{Settings.S3_ARTIFACT_PATH}/{env.get_s3_prefix()}/{Utils.normalize_string(env.JOB_NAME)}"
                for artifact in providing_artifacts:
                    if artifact.compress_zst:
                        if isinstance(artifact.path, (tuple, list)):
                            Utils.raise_with_error(
                                "TODO: list of paths is not supported with comress = True"
                            )
                        if "*" in artifact.path:
                            Utils.raise_with_error(
                                "TODO: globe is not supported with comress = True"
                            )
                        print(f"Compress artifact file [{artifact.path}]")
                        artifact.path = Utils.compress_zst(artifact.path)

                    if isinstance(artifact.path, (tuple, list)):
                        artifact_paths = artifact.path
                    else:
                        artifact_paths = [artifact.path]
                    for artifact_path in artifact_paths:
                        try:
                            assert Shell.check(
                                f"ls -l {artifact_path}", verbose=True
                            ), f"Artifact {artifact_path} not found"
                            for file_path in glob.glob(artifact_path):
                                link = S3.copy_file_to_s3(
                                    s3_path=s3_path, local_path=file_path
                                )
                                result.set_link(link)
                                artifact_links.append(link)
                        except Exception as e:
                            error = f"ERROR: Failed to upload artifact [{artifact.name}:{artifact_path}], ex [{e}]"
                            print(error)
                            info_errors.append(error)
                            result.set_status(Result.Status.ERROR)
                            is_ok = False
                if Settings.ENABLE_ARTIFACTS_REPORT and artifact_links:
                    artifact_report = {"build_urls": artifact_links}
                    print(
                        f"Artifact report enabled and will be uploaded: [{artifact_report}]"
                    )
                    artifact_report_file = f"{Settings.TEMP_DIR}/artifact_report_{Utils.normalize_string(env.JOB_NAME)}.json"
                    with open(artifact_report_file, "w", encoding="utf-8") as f:
                        json.dump(artifact_report, f)
                    link = S3.copy_file_to_s3(
                        s3_path=s3_path, local_path=artifact_report_file
                    )
                    result.set_link(link)

        ci_db = None
        if workflow.enable_cidb:
            print("Insert results to CIDB")
            try:
                ci_db = CIDB(
                    url=workflow.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
                    user=workflow.get_secret(Settings.SECRET_CI_DB_USER).get_value(),
                    passwd=workflow.get_secret(
                        Settings.SECRET_CI_DB_PASSWORD
                    ).get_value(),
                ).insert(result, result_name_for_cidb=job.result_name_for_cidb)
            except Exception as ex:
                traceback.print_exc()
                error = f"ERROR: Failed to insert data into CI DB, exception [{ex}]"
                print(error)
                info_errors.append(error)

        if env.TRACEBACKS:
            result.set_info("===\n" + "---\n".join(env.TRACEBACKS))
        result.dump()

        # always in the end
        if workflow.enable_cache:
            print(f"Run CI cache hook")
            if result.is_ok():
                CacheRunnerHooks.post_run(workflow, job)

        workflow_result = None
        if workflow.enable_report:
            print(f"Run html report hook")
            HtmlRunnerHooks.post_run(workflow, job, info_errors)
            workflow_result = Result.from_fs(workflow.name)

            if job.name == Settings.FINISH_WORKFLOW_JOB_NAME and ci_db:
                # run after HtmlRunnerHooks.post_run(), when Workflow Result has up-to-date storage_usage data
                workflow_storage_usage = StorageUsage.from_dict(
                    workflow_result.ext.get("storage_usage", {})
                )
                workflow_compute_usage = ComputeUsage.from_dict(
                    workflow_result.ext.get("compute_usage", {})
                )
                if workflow_storage_usage:
                    print(
                        "NOTE: storage_usage is found in workflow Result - insert into CIDB"
                    )
                    ci_db.insert_storage_usage(workflow_storage_usage)
                if workflow_compute_usage:
                    print(
                        "NOTE: compute_usage is found in workflow Result - insert into CIDB"
                    )
                    ci_db.insert_compute_usage(workflow_compute_usage)

        report_url = Info().get_job_report_url(latest=False)

        if workflow.enable_gh_summary_comment and (
            job.name == Settings.FINISH_WORKFLOW_JOB_NAME or not result.is_ok()
        ):
            _GH_Auth(workflow)
            try:
                summary_body = GH.ResultSummaryForGH.from_result(
                    workflow_result
                ).to_markdown()
                if not GH.post_updateable_comment(
                    comment_tags_and_bodies={"summary": summary_body},
                    only_update=True,
                ):
                    print(f"ERROR: failed to post CI summary")
            except Exception as e:
                print(f"ERROR: failed to post CI summary, ex: {e}")
                traceback.print_exc()

        if (
            workflow.enable_commit_status_on_failure and not result.is_ok()
        ) or job.enable_commit_status:
            _GH_Auth(workflow)
            if not GH.post_commit_status(
                name=job.name,
                status=result.status,
                description=result.info.splitlines()[0] if result.info else "",
                url=report_url,
            ):
                env.add_info("Failed to post GH commit status for the job")
                print(f"ERROR: Failed to post commit status for the job")

        if workflow.enable_report:
            # to make it visible in GH Actions annotations
            print(f"::notice ::Job report: {report_url}")

        if (
            workflow.enable_automerge
            and job.name == Settings.FINISH_WORKFLOW_JOB_NAME
            and workflow.is_event_pull_request()
        ):
            try:
                _GH_Auth(workflow)
                workflow_result = Result.from_fs(workflow.name)
                if workflow_result.is_ok():
                    if not GH.merge_pr():
                        print("ERROR: Failed to merge the PR")
                else:
                    print(
                        f"NOTE: Workflow status [{workflow_result.status}] - do not merge"
                    )
            except Exception as e:
                print(f"ERROR: Failed to merge the PR: [{e}]")
                traceback.print_exc()

        return is_ok

    def run(
        self,
        workflow,
        job,
        docker="",
        local_run=False,
        no_docker=False,
        param=None,
        test="",
        pr=None,
        sha=None,
        branch=None,
    ):
        res = True
        setup_env_code = -10
        prerun_code = -10
        run_code = -10

        if res and not local_run:
            print(
                f"\n\n=== Setup env script [{job.name}], workflow [{workflow.name}] ==="
            )
            try:
                setup_env_code = self._setup_env(workflow, job)
                # Source the bash script and capture the environment variables
                res = setup_env_code == 0
                if not res:
                    print(
                        f"ERROR: Setup env script failed with exit code [{setup_env_code}]"
                    )
            except Exception as e:
                print(f"ERROR: Setup env script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
            print(f"=== Setup env finished ===\n\n")
        else:
            self.generate_local_run_environment(
                workflow, job, pr=pr, sha=sha, branch=branch
            )

        if res and (not local_run or pr or sha or branch):
            res = False
            print(f"=== Pre run script [{job.name}], workflow [{workflow.name}] ===")
            try:
                prerun_code = self._pre_run(workflow, job, local_run=local_run)
                res = prerun_code == 0
                if not res:
                    print(f"ERROR: Pre-run failed with exit code [{prerun_code}]")
            except Exception as e:
                print(f"ERROR: Pre-run script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
            print(f"=== Pre run finished ===\n\n")

        if res:
            print(f"=== Run script [{job.name}], workflow [{workflow.name}] ===")
            run_code = None
            try:
                run_code = self._run(
                    workflow,
                    job,
                    docker=docker,
                    no_docker=no_docker,
                    param=param,
                    test=test,
                )
                res = run_code == 0
                if not res:
                    print(f"ERROR: Run failed with exit code [{run_code}]")
            except Exception as e:
                print(f"ERROR: Run script failed with exception [{e}]")
                traceback.print_exc()
                Info().store_traceback()
                res = False

            result = Result.from_fs(job.name)
            if not res and result.is_ok():
                # TODO: It happens due to invalid timeout handling (forceful termination by timeout does not work) - fix
                result.set_status(Result.Status.ERROR).set_info(
                    f"Job got terminated with an error, exit code [{run_code}]"
                ).dump()

            print(f"=== Run script finished ===\n\n")

        if not local_run:
            print(f"=== Post run script [{job.name}], workflow [{workflow.name}] ===")
            post_res = self._post_run(
                workflow, job, setup_env_code, prerun_code, run_code
            )
            res = res and post_res
            print(f"=== Post run script finished ===")

        if not res:
            sys.exit(1)
