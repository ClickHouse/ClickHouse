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
from .hook_cache import CacheRunnerHooks
from .hook_html import HtmlRunnerHooks
from .result import Result, ResultInfo
from .runtime import RunConfig
from .s3 import S3
from .settings import Settings
from .utils import Shell, TeePopen, Utils


class Runner:
    @staticmethod
    def generate_local_run_environment(workflow, job, pr=None, branch=None, sha=None):
        print("WARNING: Generate dummy env for local test")
        Shell.check(
            f"mkdir -p {Settings.TEMP_DIR} {Settings.INPUT_DIR} {Settings.OUTPUT_DIR}"
        )
        _Environment(
            WORKFLOW_NAME=workflow.name,
            JOB_NAME=job.name,
            REPOSITORY="",
            BRANCH=branch or Settings.MAIN_BRANCH if not pr else "",
            SHA=sha or Shell.get_output("git rev-parse HEAD"),
            PR_NUMBER=pr or -1,
            EVENT_TYPE="",
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
        )
        for docker in workflow.dockers:
            workflow_config.digest_dockers[docker.name] = Digest().calc_docker_digest(
                docker, workflow.dockers
            )

        workflow_config.dump()

        Result.generate_pending(job.name).dump()

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
        env.dump()
        print(env)

        return 0

    def _pre_run(self, workflow, job, local_run=False):
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

        print("Download required artifacts")
        required_artifacts = []
        if job.requires and workflow.artifacts:
            for requires_artifact_name in job.requires:
                for artifact in workflow.artifacts:
                    if (
                        artifact.name == requires_artifact_name
                        and artifact.type == Artifact.Type.S3
                    ):
                        required_artifacts.append(artifact)
        print(f"--- Job requires s3 artifacts [{required_artifacts}]")
        if workflow.enable_cache:
            prefixes = CacheRunnerHooks.pre_run(
                _job=job, _workflow=workflow, _required_artifacts=required_artifacts
            )
        else:
            prefixes = [env.get_s3_prefix()] * len(required_artifacts)
        for artifact, prefix in zip(required_artifacts, prefixes):
            recursive = False
            include_pattern = ""
            if "*" in artifact.path:
                s3_path = f"{Settings.S3_ARTIFACT_PATH}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/"
                recursive = True
                include_pattern = Path(artifact.path).name
                assert "*" in include_pattern
            else:
                s3_path = f"{Settings.S3_ARTIFACT_PATH}/{prefix}/{Utils.normalize_string(artifact._provided_by)}/{Path(artifact.path).name}"
            assert S3.copy_file_from_s3(
                s3_path=s3_path,
                local_path=Settings.INPUT_DIR,
                recursive=recursive,
                include_pattern=include_pattern,
            )

        return 0

    def _run(self, workflow, job, docker="", no_docker=False, param=None, test=""):
        # re-set envs for local run
        env = _Environment.get()
        env.JOB_NAME = job.name
        env.dump()

        # work around for old clickhouse jobs
        try:
            os.environ["DOCKER_TAG"] = json.dumps(
                RunConfig.from_fs(workflow.name).digest_dockers
            )
        except Exception as e:
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
            settings = [s for s in docker_settings if s.startswith("--")]
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
            docker = docker or f"{docker_name}:{docker_tag}"
            current_dir = os.getcwd()
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
            exit_code = process.wait()

            result = Result.from_fs(job.name)
            if exit_code != 0:
                if not result.is_completed():
                    if process.timeout_exceeded:
                        print(
                            f"WARNING: Job timed out: [{job.name}], timeout [{job.timeout}], exit code [{exit_code}]"
                        )
                        result.set_status(Result.Status.ERROR).set_info(
                            ResultInfo.TIMEOUT
                        )
                    elif result.is_running():
                        info = f"ERROR: Job killed, exit code [{exit_code}]  - set status to [{Result.Status.ERROR}]"
                        print(info)
                        result.set_status(Result.Status.ERROR).set_info(info)
                    else:
                        info = f"ERROR: Invalid status [{result.status}] for exit code [{exit_code}]  - switch to [{Result.Status.ERROR}]"
                        print(info)
                        result.set_status(Result.Status.ERROR).set_info(info)
            result.dump()

        return exit_code

    def _post_run(
        self, workflow, job, setup_env_exit_code, prerun_exit_code, run_exit_code
    ):
        info_errors = []
        env = _Environment.get()
        result_exist = Result.exist(job.name)

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
            info = f"ERROR: {ResultInfo.PRE_JOB_FAILED}"
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
            result = Result.create_from(
                status=Result.Status.ERROR,
                info=f"Failed to read Result json, ex: [{e}]",
            ).dump()

        if not result.is_completed():
            info = f"ERROR: {ResultInfo.KILLED}"
            print(info)
            result.set_info(info).set_status(Result.Status.ERROR).dump()

        result.update_duration().dump()

        if result.is_error():
            result.set_files([Settings.RUN_LOG])

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
                for artifact in providing_artifacts:
                    try:
                        assert Shell.check(
                            f"ls -l {artifact.path}", verbose=True
                        ), f"Artifact {artifact.path} not found"
                        s3_path = f"{Settings.S3_ARTIFACT_PATH}/{env.get_s3_prefix()}/{Utils.normalize_string(env.JOB_NAME)}"
                        for file_path in glob.glob(artifact.path):
                            link = S3.copy_file_to_s3(
                                s3_path=s3_path, local_path=file_path
                            )
                            result.set_link(link)
                    except Exception as e:
                        error = (
                            f"ERROR: Failed to upload artifact [{artifact}], ex [{e}]"
                        )
                        print(error)
                        info_errors.append(error)
                        result.set_status(Result.Status.ERROR)

        if workflow.enable_cidb:
            print("Insert results to CIDB")
            try:
                CIDB(
                    url=workflow.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
                    passwd=workflow.get_secret(
                        Settings.SECRET_CI_DB_PASSWORD
                    ).get_value(),
                ).insert(result)
            except Exception as ex:
                error = f"ERROR: Failed to insert data into CI DB, exception [{ex}]"
                print(error)
                info_errors.append(error)

        result.dump()

        # always in the end
        if workflow.enable_cache:
            print(f"Run CI cache hook")
            if result.is_ok():
                CacheRunnerHooks.post_run(workflow, job)

        if workflow.enable_report:
            print(f"Run html report hook")
            HtmlRunnerHooks.post_run(workflow, job, info_errors)

        return True

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
            print(f"=== Setup env finished ===\n\n")
        else:
            self.generate_local_run_environment(
                workflow, job, pr=pr, branch=branch, sha=sha
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
            print(f"=== Pre run finished ===\n\n")

        if res:
            res = False
            print(f"=== Run script [{job.name}], workflow [{workflow.name}] ===")
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
            print(f"=== Run scrip finished ===\n\n")

        if not local_run:
            print(f"=== Post run script [{job.name}], workflow [{workflow.name}] ===")
            self._post_run(workflow, job, setup_env_code, prerun_code, run_code)
            print(f"=== Post run scrip finished ===")

        if not res:
            sys.exit(1)
