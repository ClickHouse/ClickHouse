import sys
from typing import Dict

from praktika import Job, Workflow
from praktika._environment import _Environment
from praktika.cidb import CIDB
from praktika.digest import Digest
from praktika.docker import Docker
from praktika.gh import GH
from praktika.hook_cache import CacheRunnerHooks
from praktika.hook_html import HtmlRunnerHooks
from praktika.mangle import _get_workflows
from praktika.result import Result, ResultInfo
from praktika.runtime import RunConfig
from praktika.s3 import S3
from praktika.settings import Settings
from praktika.utils import Shell, Utils

assert Settings.CI_CONFIG_RUNS_ON

_workflow_config_job = Job.Config(
    name=Settings.CI_CONFIG_JOB_NAME,
    runs_on=Settings.CI_CONFIG_RUNS_ON,
    job_requirements=(
        Job.Requirements(
            python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
            python_requirements_txt=Settings.INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS,
        )
        if Settings.INSTALL_PYTHON_REQS_FOR_NATIVE_JOBS
        else None
    ),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.CI_CONFIG_JOB_NAME}'",
)

_docker_build_job = Job.Config(
    name=Settings.DOCKER_BUILD_JOB_NAME,
    runs_on=Settings.DOCKER_BUILD_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    timeout=4 * 3600,
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.DOCKER_BUILD_JOB_NAME}'",
)

_final_job = Job.Config(
    name=Settings.FINISH_WORKFLOW_JOB_NAME,
    runs_on=Settings.CI_CONFIG_RUNS_ON,
    job_requirements=Job.Requirements(
        python=Settings.INSTALL_PYTHON_FOR_NATIVE_JOBS,
        python_requirements_txt="",
    ),
    command=f"{Settings.PYTHON_INTERPRETER} -m praktika.native_jobs '{Settings.FINISH_WORKFLOW_JOB_NAME}'",
    run_unless_cancelled=True,
)


def _build_dockers(workflow, job_name):
    print(f"Start [{job_name}], workflow [{workflow.name}]")
    dockers = workflow.dockers
    ready = []
    results = []
    job_status = Result.Status.SUCCESS
    job_info = ""
    dockers = Docker.sort_in_build_order(dockers)
    docker_digests = {}  # type: Dict[str, str]
    for docker in dockers:
        docker_digests[docker.name] = Digest().calc_docker_digest(docker, dockers)

    if not Shell.check(
        "docker buildx inspect --bootstrap | grep -q docker-container", verbose=True
    ):
        print("Install docker container driver")
        if not Shell.check(
            "docker buildx create --use --name mybuilder --driver docker-container",
            verbose=True,
        ):
            job_status = Result.Status.FAILED
            job_info = "Failed to install docker buildx driver"

    if job_status == Result.Status.SUCCESS:
        if not Docker.login(
            Settings.DOCKERHUB_USERNAME,
            user_password=workflow.get_secret(Settings.DOCKERHUB_SECRET).get_value(),
        ):
            job_status = Result.Status.FAILED
            job_info = "Failed to login to dockerhub"

    if job_status == Result.Status.SUCCESS:
        for docker in dockers:
            assert (
                docker.name not in ready
            ), f"All docker names must be uniq [{dockers}]"
            stopwatch = Utils.Stopwatch()
            info = f"{docker.name}:{docker_digests[docker.name]}"
            log_file = f"{Settings.OUTPUT_DIR}/docker_{Utils.normalize_string(docker.name)}.log"
            files = []

            code, out, err = Shell.get_res_stdout_stderr(
                f"docker manifest inspect {docker.name}:{docker_digests[docker.name]}"
            )
            print(
                f"Docker inspect results for {docker.name}:{docker_digests[docker.name]}: exit code [{code}], out [{out}], err [{err}]"
            )
            if "no such manifest" in err:
                ret_code = Docker.build(
                    docker, log_file=log_file, digests=docker_digests, add_latest=False
                )
                if ret_code == 0:
                    status = Result.Status.SUCCESS
                else:
                    status = Result.Status.FAILED
                    job_status = Result.Status.FAILED
                    info += f", failed with exit code: {ret_code}, see log"
                    files.append(log_file)
            else:
                print(
                    f"Docker image [{docker.name}:{docker_digests[docker.name]} exists - skip build"
                )
                status = Result.Status.SKIPPED
            ready.append(docker.name)
            results.append(
                Result(
                    name=docker.name,
                    status=status,
                    info=info,
                    duration=stopwatch.duration,
                    start_time=stopwatch.start_time,
                    files=files,
                )
            )
    Result.from_fs(job_name).set_status(job_status).set_results(results).set_info(
        job_info
    )

    if job_status != Result.Status.SUCCESS:
        sys.exit(1)


def _config_workflow(workflow: Workflow.Config, job_name):
    def _check_yaml_up_to_date():
        print("Check workflows are up to date")
        stop_watch = Utils.Stopwatch()
        exit_code, output, err = Shell.get_res_stdout_stderr(
            f"git diff-index HEAD -- {Settings.WORKFLOW_PATH_PREFIX}"
        )
        info = ""
        status = Result.Status.SUCCESS
        if exit_code != 0:
            info = f"workspace has uncommitted files unexpectedly [{output}]"
            status = Result.Status.ERROR
            print("ERROR: ", info)
        else:
            Shell.check(f"{Settings.PYTHON_INTERPRETER} -m praktika --generate")
            exit_code, output, err = Shell.get_res_stdout_stderr(
                f"git diff-index HEAD -- {Settings.WORKFLOW_PATH_PREFIX}"
            )
            if exit_code != 0:
                info = f"workspace has outdated workflows [{output}] - regenerate with [python -m praktika --generate]"
                status = Result.Status.ERROR
                print("ERROR: ", info)

        return (
            Result(
                name="Check Workflows updated",
                status=status,
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
                info=info,
            ),
            info,
        )

    def _check_secrets(secrets):
        print("Check Secrets")
        stop_watch = Utils.Stopwatch()
        infos = []
        for secret_config in secrets:
            value = secret_config.get_value()
            if not value:
                info = f"ERROR: Failed to read secret [{secret_config.name}]"
                infos.append(info)
                print(info)

        info = "\n".join(infos)
        return (
            Result(
                name="Check Secrets",
                status=(Result.Status.FAILED if infos else Result.Status.SUCCESS),
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
                info=info,
            ),
            info,
        )

    def _check_db(workflow):
        stop_watch = Utils.Stopwatch()
        res, info = CIDB(
            workflow.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
            workflow.get_secret(Settings.SECRET_CI_DB_PASSWORD).get_value(),
        ).check()
        return (
            Result(
                name="Check CI DB",
                status=(Result.Status.FAILED if not res else Result.Status.SUCCESS),
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
                info=info,
            ),
            info,
        )

    print(f"Start [{job_name}], workflow [{workflow.name}]")
    results = []
    files = []
    info_lines = []
    job_status = Result.Status.SUCCESS

    workflow_config = RunConfig(
        name=workflow.name,
        digest_jobs={},
        digest_dockers={},
        sha=_Environment.get().SHA,
        cache_success=[],
        cache_success_base64=[],
        cache_artifacts={},
    ).dump()

    # checks:
    result_, info = _check_yaml_up_to_date()
    if result_.status != Result.Status.SUCCESS:
        print("ERROR: yaml files are outdated - regenerate, commit and push")
        job_status = Result.Status.ERROR
        info_lines.append(job_name + ": " + info)
    results.append(result_)

    if workflow.secrets:
        result_, info = _check_secrets(workflow.secrets)
        if result_.status != Result.Status.SUCCESS:
            print(f"ERROR: Invalid secrets in workflow [{workflow.name}]")
            job_status = Result.Status.ERROR
            info_lines.append(job_name + ": " + info)
        results.append(result_)

    if workflow.enable_cidb:
        result_, info = _check_db(workflow)
        if result_.status != Result.Status.SUCCESS:
            job_status = Result.Status.ERROR
            info_lines.append(job_name + ": " + info)
        results.append(result_)

    # config:
    if workflow.dockers:
        print("Calculate docker's digests")
        dockers = workflow.dockers
        dockers = Docker.sort_in_build_order(dockers)
        for docker in dockers:
            workflow_config.digest_dockers[docker.name] = Digest().calc_docker_digest(
                docker, dockers
            )
        workflow_config.dump()

    if workflow.enable_cache:
        print("Cache Lookup")
        stop_watch = Utils.Stopwatch()
        workflow_config = CacheRunnerHooks.configure(workflow)
        results.append(
            Result(
                name="Cache Lookup",
                status=Result.Status.SUCCESS,
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
            )
        )
        files.append(RunConfig.file_name_static(workflow.name))

    workflow_config.dump()

    if workflow.enable_report:
        print("Init report")
        stop_watch = Utils.Stopwatch()
        HtmlRunnerHooks.configure(workflow)
        results.append(
            Result(
                name="Init Report",
                status=Result.Status.SUCCESS,
                start_time=stop_watch.start_time,
                duration=stop_watch.duration,
            )
        )
        files.append(Result.file_name_static(workflow.name))

    Result.from_fs(job_name).set_status(job_status).set_results(results).set_files(
        files
    ).set_info("\n".join(info_lines))

    if job_status != Result.Status.SUCCESS:
        sys.exit(1)


def _finish_workflow(workflow, job_name):
    print(f"Start [{job_name}], workflow [{workflow.name}]")
    env = _Environment.get()

    print("Check Actions statuses")
    print(env.get_needs_statuses())

    print("Check Workflow results")
    S3.copy_result_from_s3(
        Result.file_name_static(workflow.name),
        lock=False,
    )
    workflow_result = Result.from_fs(workflow.name)

    ready_for_merge_status = Result.Status.SUCCESS
    ready_for_merge_description = ""
    failed_results = []
    update_final_report = False
    for result in workflow_result.results:
        if result.name == job_name or result.status in (
            Result.Status.SUCCESS,
            Result.Status.SKIPPED,
        ):
            continue
        if not result.is_completed():
            print(
                f"ERROR: not finished job [{result.name}] in the workflow - set status to error"
            )
            result.status = Result.Status.ERROR
            # dump workflow result after update - to have an updated result in post
            workflow_result.dump()
            # add error into env - should apper in the report
            env.add_info(ResultInfo.NOT_FINALIZED + f" [{result.name}]")
            update_final_report = True
        job = workflow.get_job(result.name)
        if not job or not job.allow_merge_on_failure:
            print(
                f"NOTE: Result for [{result.name}] has not ok status [{result.status}]"
            )
            ready_for_merge_status = Result.Status.FAILED
            failed_results.append(result.name.split("(", maxsplit=1)[0])  # cut name

    if failed_results:
        ready_for_merge_description = f"failed: {', '.join(failed_results)}"

    if not GH.post_commit_status(
        name=Settings.READY_FOR_MERGE_STATUS_NAME + f" [{workflow.name}]",
        status=ready_for_merge_status,
        description=ready_for_merge_description,
        url="",
    ):
        print(f"ERROR: failed to set status [{Settings.READY_FOR_MERGE_STATUS_NAME}]")
        env.add_info(ResultInfo.GH_STATUS_ERROR)

    if update_final_report:
        S3.copy_result_to_s3(
            workflow_result,
            unlock=False,
        )  # no lock - no unlock

    Result.from_fs(job_name).set_status(Result.Status.SUCCESS).set_info(
        ready_for_merge_description
    )


if __name__ == "__main__":
    job_name = sys.argv[1]
    assert job_name, "Job name must be provided as input argument"
    workflow = _get_workflows(name=_Environment.get().WORKFLOW_NAME)[0]
    if job_name == Settings.DOCKER_BUILD_JOB_NAME:
        _build_dockers(workflow, job_name)
    elif job_name == Settings.CI_CONFIG_JOB_NAME:
        _config_workflow(workflow, job_name)
    elif job_name == Settings.FINISH_WORKFLOW_JOB_NAME:
        _finish_workflow(workflow, job_name)
    else:
        assert False, f"BUG, job name [{job_name}]"
