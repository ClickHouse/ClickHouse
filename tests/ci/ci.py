import argparse
import concurrent.futures
import json
import logging
import os
import re
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import docker_images_helper
from ci_buddy import CIBuddy
from ci_cache import CiCache
from ci_config import BUILD_NAMES_MAPPING, CI
from ci_metadata import CiMetadata
from ci_settings import CiSettings
from ci_utils import GH, Envs, Utils
from commit_status_helper import (
    CommitStatusData,
    RerunHelper,
    format_description,
    get_commit,
    get_commit_filtered_statuses,
    set_status_comment,
)
from digest_helper import DockerDigester
from env_helper import GITHUB_REPOSITORY, IS_CI, TEMP_PATH
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from pr_info import PRInfo
from report import ERROR, PENDING, SUCCESS, BuildResult, JobReport, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen

# pylint: disable=too-many-lines,too-many-branches


def get_check_name(check_name: str, batch: int, num_batches: int) -> str:
    res = check_name
    if num_batches > 1:
        res = f"{check_name} [{batch+1}/{num_batches}]"
    return res


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--cancel-previous-run",
        action="store_true",
        help="Action that cancels previous running PR workflow if PR added into the Merge Queue",
    )
    parser.add_argument(
        "--set-pending-status",
        action="store_true",
        help="Action to set needed pending statuses in the beginning of CI workflow, e.g. for Sync wf",
    )
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Action that configures ci run. Calculates digests, checks job to be executed, generates json output",
    )
    parser.add_argument(
        "--workflow",
        default="",
        type=str,
        help="Workflow Name, to be provided with --configure for workflow-specific CI runs",
    )
    parser.add_argument(
        "--update-gh-statuses",
        action="store_true",
        help="Action that recreate success GH statuses for jobs that finished successfully in past and will be "
        "skipped this time",
    )
    parser.add_argument(
        "--pre",
        action="store_true",
        help="Action that executes prerequisites for the job provided in --job-name",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given "
        "job name.",
    )
    parser.add_argument(
        "--run-from-praktika",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given "
        "job name.",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Action that executes post actions for the job provided in --job-name",
    )
    parser.add_argument(
        "--mark-success",
        action="store_true",
        help="Action that marks job provided in --job-name (with batch provided in --batch) as successful",
    )
    parser.add_argument(
        "--job-name",
        default="",
        type=str,
        help="Job name as in config",
    )
    parser.add_argument(
        "--run-command",
        default="",
        type=str,
        help="A run command to run in --run action. Will override run_command from a job config if any",
    )
    parser.add_argument(
        "--batch",
        default=-1,
        type=int,
        help="Current batch number (required for --mark-success), -1 or omit for single-batch job",
    )
    parser.add_argument(
        "--infile",
        default="",
        type=str,
        help="Input json file or json string with ci run config",
    )
    parser.add_argument(
        "--outfile",
        default="",
        type=str,
        required=False,
        help="output file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes json output pretty formatted",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        default=False,
        help="skip fetching docker data from dockerhub, used in --configure action (for debugging)",
    )
    parser.add_argument(
        "--docker-digest-or-latest",
        action="store_true",
        default=False,
        help="temporary hack to fallback to latest if image with digest as a tag is not on docker hub",
    )
    parser.add_argument(
        "--skip-jobs",
        action="store_true",
        default=False,
        help="skip fetching data about job runs, used in --configure action (for debugging and nightly ci)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Used with --run, force the job to run, omitting the ci cache",
    )
    # FIXME: remove, not used
    parser.add_argument(
        "--rebuild-all-binaries",
        action="store_true",
        default=False,
        help="[DEPRECATED. to be removed, once no wf use it] will create run config without skipping build jobs in "
        "any case, used in --configure action (for release branches)",
    )
    parser.add_argument(
        "--commit-message",
        default="",
        help="debug option to test commit message processing",
    )
    return parser.parse_args()


# FIXME: rewrite the docker job as regular reusable_test job and move interaction with docker hub inside job script
#   that way run config will be more clean, workflow more generic and less api calls to dockerhub
def check_missing_images_on_dockerhub(
    image_name_tag: Dict[str, str], arch: Optional[str] = None
) -> Dict[str, str]:
    """
    Checks missing images on dockerhub.
    Works concurrently for all given images.
    Docker must be logged in.
    """

    def run_docker_command(
        image: str, image_digest: str, arch: Optional[str] = None
    ) -> Dict:
        """
        aux command for fetching single docker manifest
        """
        command = [
            "docker",
            "manifest",
            "inspect",
            f"{image}:{image_digest}" if not arch else f"{image}:{image_digest}-{arch}",
        ]

        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        return {
            "image": image,
            "image_digest": image_digest,
            "arch": arch,
            "stdout": process.stdout,
            "stderr": process.stderr,
            "return_code": process.returncode,
        }

    result: Dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_docker_command, image, tag, arch)
            for image, tag in image_name_tag.items()
        ]

        responses = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
        for resp in responses:
            name, stdout, stderr, digest, arch = (
                resp["image"],
                resp["stdout"],
                resp["stderr"],
                resp["image_digest"],
                resp["arch"],
            )
            if stderr:
                if stderr.startswith("no such manifest"):
                    result[name] = digest
                else:
                    print(f"Error: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Error: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Error: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


def _pre_action(s3, job_name, batch, indata, pr_info):
    no_cache = CiSettings.create_from_run_config(indata).no_ci_cache
    print("Clear dmesg")
    Utils.clear_dmesg()
    CommitStatusData.cleanup()
    JobReport.cleanup()
    BuildResult.cleanup()
    ci_cache = CiCache(s3, indata["jobs_data"]["digests"])

    # for release/master branches reports must be from the same branch
    report_prefix = ""
    if pr_info.is_master or pr_info.is_release:
        # do not set report prefix for scheduled or dispatched wf (in case it started from feature branch while
        #   testing), otherwise reports won't be found
        if not (pr_info.is_scheduled or pr_info.is_dispatched):
            report_prefix = Utils.normalize_string(pr_info.head_ref)
    print(
        f"Use report prefix [{report_prefix}], pr_num [{pr_info.number}], head_ref [{pr_info.head_ref}]"
    )
    reports_files = ci_cache.download_build_reports(file_prefix=report_prefix)

    ci_cache.dump_run_config(indata)

    to_be_skipped = False
    skip_status = SUCCESS
    # check if job was run already
    if CI.is_build_job(job_name):
        # this is a build job - check if a build report is present
        build_result = (
            BuildResult.load_any(job_name, pr_info.number, pr_info.head_ref)
            if not no_cache
            else None
        )
        if build_result:
            if build_result.status == SUCCESS:
                to_be_skipped = True
            else:
                print(
                    "Build report found but status is unsuccessful - will try to rerun"
                )
            print("::group::Build Report")
            print(build_result.as_json())
            print("::endgroup::")
    else:
        # this is a test job - check if GH commit status or cache record is present
        commit = get_commit(GitHub(get_best_robot_token(), per_page=100), pr_info.sha)
        # rerun helper check
        # FIXME: Find a way to identify if job restarted manually (by developer) or by automatic workflow restart (died spot-instance)
        #  disable rerun check for the former
        if job_name not in (
            CI.JobNames.BUILD_CHECK,
        ):  # we might want to rerun build report job
            rerun_helper = RerunHelper(commit, _get_ext_check_name(job_name))
            if rerun_helper.is_already_finished_by_status():
                print(
                    f"WARNING: Rerunning job with GH status, rerun triggered by {Envs.GITHUB_ACTOR}"
                )
                status = rerun_helper.get_finished_status()
                assert status
                print("::group::Commit Status")
                print(status)
                print("::endgroup::")
                to_be_skipped = True
                skip_status = status.state

        # ci cache check
        if not to_be_skipped and not no_cache and not Utils.is_job_triggered_manually():
            ci_cache = CiCache(s3, indata["jobs_data"]["digests"]).update()
            job_config = CI.get_job_config(job_name)
            if ci_cache.is_successful(
                job_name,
                batch,
                job_config.num_batches,
                job_config.required_on_release_branch,
            ):
                print("CICache record has be found - job will be skipped")
                job_status = ci_cache.get_successful(
                    job_name, batch, job_config.num_batches
                )
                assert job_status, "BUG"
                _create_gh_status(
                    commit,
                    job_name,
                    batch,
                    job_config.num_batches,
                    job_status,
                )
                to_be_skipped = True
                # skip_status = SUCCESS already there
                GH.print_in_group("Commit Status Data", job_status)

    # create dummy report
    jr = JobReport.create_dummy(status=skip_status, job_skipped=to_be_skipped)
    jr.dump()

    print(f"Pre action done. Report files [{reports_files}] have been downloaded")


def _mark_success_action(
    s3: S3Helper,
    indata: Dict[str, Any],
    pr_info: PRInfo,
    job: str,
    batch: int,
) -> None:
    ci_cache = CiCache(s3, indata["jobs_data"]["digests"])
    job_config = CI.get_job_config(job)
    num_batches = job_config.num_batches
    # if batch is not provided - set to 0
    batch = 0 if batch == -1 else batch
    assert (
        0 <= batch < num_batches
    ), f"--batch must be provided and in range [0, {num_batches}) for {job}"

    # FIXME: find generic design for propagating and handling job status (e.g. stop using statuses in GH api)
    #   now job ca be build job w/o status data, any other job that exit with 0 with or w/o status data
    if CI.is_build_job(job):
        # there is no CommitStatus for build jobs
        # create dummy status relying on JobReport
        # FIXME: consider creating commit status for build jobs too, to treat everything the same way
        job_report = JobReport.load() if JobReport.exist() else None
        if job_report and job_report.status == SUCCESS:
            CommitStatusData(
                SUCCESS,
                "dummy description",
                "dummy_url",
                pr_num=pr_info.number,
                sha=pr_info.sha,
            ).dump_status()

    job_status = None
    if CommitStatusData.exist():
        # normal scenario
        job_status = CommitStatusData.load_status()
    else:
        # apparently exit after rerun-helper check
        # do nothing, exit without failure
        print(f"ERROR: no status file for job [{job}]")

    if job_config.run_by_labels or not job_config.has_digest():
        print(f"Job [{job}] has no digest or run by label in CI - do not cache")
    else:
        if pr_info.is_master:
            pass
            # delete method is disabled for ci_cache. need it?
            # pending enabled for master branch jobs only
            # ci_cache.delete_pending(job, batch, num_batches, release_branch=True)
        if job_status and job_status.is_ok():
            ci_cache.push_successful(
                job, batch, num_batches, job_status, pr_info.is_release
            )
            print(f"Job [{job}] is ok")
        elif job_status and not job_status.is_ok():
            ci_cache.push_failed(
                job, batch, num_batches, job_status, pr_info.is_release
            )
            print(f"Job [{job}] is failed with status [{job_status.status}]")
        else:
            job_status = CommitStatusData(
                description="dummy description", status=ERROR, report_url="dummy url"
            )
            ci_cache.push_failed(
                job, batch, num_batches, job_status, pr_info.is_release
            )
            print(f"No CommitStatusData for [{job}], push dummy failure to ci_cache")


def _print_results(result: Any, outfile: Optional[str], pretty: bool = False) -> None:
    if outfile:
        with open(outfile, "w", encoding="utf-8") as f:
            if isinstance(result, str):
                print(result, file=f)
            elif isinstance(result, dict):
                print(json.dumps(result, indent=2 if pretty else None), file=f)
            else:
                raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    else:
        if isinstance(result, str):
            print(result)
        elif isinstance(result, dict):
            print(json.dumps(result, indent=2 if pretty else None))
        else:
            raise AssertionError(f"Unexpected type for 'res': {type(result)}")


def _configure_docker_jobs(docker_digest_or_latest: bool) -> Dict:
    print("::group::Docker images check")
    # generate docker jobs data
    docker_digester = DockerDigester()
    imagename_digest_dict = (
        docker_digester.get_all_digests()
    )  # 'image name - digest' mapping
    images_info = docker_images_helper.get_images_info()

    # FIXME: we need login as docker manifest inspect goes directly to one of the *.docker.com hosts instead of "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
    #   find if it's possible to use the setting of /etc/docker/daemon.json (https://github.com/docker/cli/issues/4484#issuecomment-1688095463)
    docker_images_helper.docker_login()
    missing_multi_dict = check_missing_images_on_dockerhub(imagename_digest_dict)
    missing_multi = list(missing_multi_dict)
    missing_amd64 = []
    missing_aarch64 = []
    if not docker_digest_or_latest:
        # look for missing arm and amd images only among missing multi-arch manifests @missing_multi_dict
        # to avoid extra dockerhub api calls
        missing_amd64 = list(
            check_missing_images_on_dockerhub(missing_multi_dict, "amd64")
        )
        # FIXME: WA until full arm support: skip not supported arm images
        missing_aarch64 = list(
            check_missing_images_on_dockerhub(
                {
                    im: digest
                    for im, digest in missing_multi_dict.items()
                    if not images_info[im]["only_amd64"]
                },
                "aarch64",
            )
        )
    else:
        if missing_multi:
            assert False, f"Missing images [{missing_multi}], cannot proceed"
    print("::endgroup::")

    return {
        "images": imagename_digest_dict,
        "missing_aarch64": missing_aarch64,
        "missing_amd64": missing_amd64,
        "missing_multi": missing_multi,
    }


def _configure_jobs(
    s3: S3Helper,
    pr_info: PRInfo,
    ci_settings: CiSettings,
    skip_jobs: bool,
    workflow_name: str = "",
    dry_run: bool = False,
) -> CiCache:
    """
    returns CICache instance with configured job's data
    :param s3:
    :param pr_info:
    :param ci_settings:
    :return:
    """

    # get all jobs
    if not skip_jobs:
        job_configs = CI.get_workflow_jobs_with_configs(
            is_mq=pr_info.is_merge_queue,
            is_docs_only=pr_info.has_changes_in_documentation_only(),
            is_master=pr_info.is_master,
            is_pr=pr_info.is_pr,
            workflow_name=workflow_name,
        )
    else:
        job_configs = {}

    if not workflow_name:
        # filter jobs in accordance with ci settings
        job_configs = ci_settings.apply(
            job_configs,
            pr_info.is_release,
            is_pr=pr_info.is_pr,
            is_mq=pr_info.is_merge_queue,
            labels=pr_info.labels,
        )

    # add all job batches to job's to_do batches
    for _job, job_config in job_configs.items():
        batches = []
        for batch in range(job_config.num_batches):
            batches.append(batch)
        job_config.batches = batches

    # check jobs in ci cache
    ci_cache = CiCache.calc_digests_and_create(
        s3,
        job_configs,
        cache_enabled=not ci_settings.no_ci_cache and not skip_jobs and IS_CI,
        dry_run=dry_run,
    )
    ci_cache.update()
    ci_cache.apply(job_configs, is_release=pr_info.is_release)

    return ci_cache


def _generate_ci_stage_config(
    jobs_data: Dict[str, Any], non_blocking_mode: bool = False
) -> Dict[str, Dict[str, Any]]:
    """
    populates GH Actions' workflow with real jobs
    "Builds_1": [{"job_name": NAME, "runner_type": RUNNER_TYPE}]
    "Tests_1": [{"job_name": NAME, "runner_type": RUNNER_TYPE}]
    ...
    """
    result = {}  # type: Dict[str, Any]
    stages_to_do = []
    for job in jobs_data:
        stage_type = CI.get_job_ci_stage(job, non_blocking_ci=non_blocking_mode)
        if stage_type == CI.WorkflowStages.NA:
            continue
        if stage_type not in result:
            result[stage_type] = []
            stages_to_do.append(stage_type)
        result[stage_type].append(
            {"job_name": job, "runner_type": CI.JOB_CONFIGS[job].runner_type}
        )
    result["stages_to_do"] = stages_to_do
    return result


def _create_gh_status(
    commit: Any, job: str, batch: int, num_batches: int, job_status: CommitStatusData
) -> None:
    print(f"Going to re-create GH status for job [{job}]")
    assert job_status.status == SUCCESS, "BUG!"
    commit.create_status(
        state=job_status.status,
        target_url=job_status.report_url,
        description=format_description(
            f"Reused from [{job_status.pr_num}-{job_status.sha[0:8]}]: "
            f"{job_status.description}"
        ),
        context=get_check_name(job, batch=batch, num_batches=num_batches),
    )


def _update_gh_statuses_action(indata: Dict, s3: S3Helper) -> None:
    if CiSettings.create_from_run_config(indata).no_ci_cache:
        print("CI cache is disabled - skip restoring commit statuses from CI cache")
        return
    job_digests = indata["jobs_data"]["digests"]
    jobs_to_skip = indata["jobs_data"]["jobs_to_skip"]
    jobs_to_do = indata["jobs_data"]["jobs_to_do"]
    ci_cache = CiCache(s3, job_digests).update().fetch_records_data().print_status()

    # create GH status
    pr_info = PRInfo()
    commit = get_commit(GitHub(get_best_robot_token(), per_page=100), pr_info.sha)

    def _concurrent_create_status(job: str, batch: int, num_batches: int) -> None:
        job_status = ci_cache.get_successful(job, batch, num_batches)
        if not job_status:
            return
        _create_gh_status(commit, job, batch, num_batches, job_status)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for job in job_digests:
            if job not in jobs_to_skip and job not in jobs_to_do:
                # no need to create status for job that are not supposed to be executed
                continue
            if CI.is_build_job(job):
                # no GH status for build jobs
                continue
            try:
                job_config = CI.get_job_config(job)
            except Exception as e:
                print(
                    f"WARNING: Failed to get job config for [{job}], it might have been removed from main branch, ex: [{e}]"
                )
                continue
            for batch in range(job_config.num_batches):
                future = executor.submit(
                    _concurrent_create_status, job, batch, job_config.num_batches
                )
                futures.append(future)
        done, _ = concurrent.futures.wait(futures)
        for future in done:
            try:
                _ = future.result()
            except Exception as e:
                raise e
    print("Going to update overall CI report")
    try:
        set_status_comment(commit, pr_info)
    except Exception as e:
        print(f"WARNING: Failed to update CI Running status, ex [{e}]")
    print("... CI report update - done")


def _fetch_commit_tokens(message: str, pr_info: PRInfo) -> List[str]:
    pattern = r"(#|- \[x\] +<!---)(\w+)"
    matches = [match[-1] for match in re.findall(pattern, message)]
    res = [
        match
        for match in matches
        if match in CI.Tags or match.startswith("job_") or match.startswith("batch_")
    ]
    print(f"CI modifiers from commit message: [{res}]")
    res_2 = []
    if pr_info.is_pr:
        matches = [match[-1] for match in re.findall(pattern, pr_info.body)]
        res_2 = [
            match
            for match in matches
            if match in CI.Tags
            or match.startswith("job_")
            or match.startswith("batch_")
        ]
        print(f"CI modifiers from PR body: [{res_2}]")
    return list(set(res + res_2))


def _run_test(job_name: str, run_command: str) -> int:
    assert (
        run_command or CI.get_job_config(job_name).run_command
    ), "Run command must be provided as input argument or be configured in job config"

    env = os.environ.copy()
    timeout = CI.get_job_config(job_name).timeout or None

    if not run_command:
        run_command = "/".join(
            (os.path.dirname(__file__), CI.get_job_config(job_name).run_command)
        )
        if ".py" in run_command and not run_command.startswith("python"):
            run_command = "python3 " + run_command
        print("Use run command from a job config")
    else:
        print("Use run command from the workflow")
    env["CHECK_NAME"] = job_name
    env["MAX_RUN_TIME"] = str(timeout or 0)
    print(f"Going to start run command [{run_command}]")
    stopwatch = Stopwatch()
    job_log = Path(TEMP_PATH) / "job_log.txt"
    with TeePopen(run_command, job_log, env, timeout) as process:
        print(f"Job process started, pid [{process.process.pid}]")
        retcode = process.wait()
        if retcode != 0:
            print(f"Run action failed for: [{job_name}] with exit code [{retcode}]")
        if process.timeout_exceeded:
            print(f"Job timed out: [{job_name}] exit code [{retcode}]")
            assert JobReport.exist(), "JobReport real or dummy must be present"
            jr = JobReport.load()
            if jr.dummy:
                print(
                    "ERROR: Run action failed with timeout and did not generate JobReport - update dummy report with execution time"
                )
                jr.test_results = [TestResult.create_check_timeout_expired()]
                jr.duration = stopwatch.duration_seconds
                jr.additional_files += [job_log]

    print(f"Run action done for: [{job_name}]")
    return retcode


def _get_ext_check_name(check_name: str) -> str:
    run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
    run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    if run_by_hash_total > 1:
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        check_name_with_group = check_name
    return check_name_with_group


def _cancel_pr_workflow(
    s3: S3Helper, pr_number: int, cancel_sync: bool = False
) -> None:
    wf_data = CiMetadata(s3, pr_number).fetch_meta()
    if not cancel_sync:
        if not wf_data.run_id:
            print(f"ERROR: FIX IT: Run id has not been found PR [{pr_number}]!")
        else:
            print(
                f"Canceling PR workflow run_id: [{wf_data.run_id}], pr: [{pr_number}]"
            )
            GitHub.cancel_wf(GITHUB_REPOSITORY, wf_data.run_id, get_best_robot_token())
    else:
        if not wf_data.sync_pr_run_id:
            print("WARNING: Sync PR run id has not been found")
        else:
            print(f"Canceling sync PR workflow run_id: [{wf_data.sync_pr_run_id}]")
            GitHub.cancel_wf(
                "ClickHouse/clickhouse-private",
                wf_data.sync_pr_run_id,
                get_best_robot_token(),
            )


def _set_pending_statuses(pr_info: PRInfo) -> None:
    commit = get_commit(GitHub(get_best_robot_token(), per_page=100), pr_info.sha)
    try:
        found = False
        statuses = get_commit_filtered_statuses(commit)
        for commit_status in statuses:
            if commit_status.context == CI.StatusNames.SYNC:
                print(
                    f"Sync status found [{commit_status.state}], [{commit_status.description}] - won't be overwritten"
                )
                found = True
                break
        if not found:
            print("Set Sync status to pending")
            commit.create_status(
                state=PENDING,
                target_url="",
                description=CI.SyncState.PENDING,
                context=CI.StatusNames.SYNC,
            )
    except Exception as ex:
        print(f"ERROR: failed to set GH commit status, ex: {ex}")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if args.mark_success or args.pre or args.run:
        assert args.infile, "Run config must be provided via --infile"
        assert args.job_name, "Job name must be provided via --job-name"

    indata: Optional[Dict[str, Any]] = None
    if args.infile:
        if os.path.isfile(args.infile):
            with open(args.infile, encoding="utf-8") as jfd:
                indata = json.load(jfd)
        else:
            indata = json.loads(args.infile)
        assert indata and isinstance(indata, dict), "Invalid --infile json"

    result: Dict[str, Any] = {}

    ### CONFIGURE action: start
    if (
        args.configure
        or args.pre
        or args.run
        or args.post
        or args.mark_success
        or args.update_gh_statuses
        or args.cancel_previous_run
        or args.set_pending_status
    ):
        assert False, "Obsolete"

    # TODO: migrate all jobs and remove
    ### RUN action for migration to praktika: start
    # temporary mode for migration to new ci workflow
    elif args.run_from_praktika:
        check_name = os.environ["JOB_NAME"]
        check_name = BUILD_NAMES_MAPPING.get(check_name, check_name)
        assert check_name
        os.environ["CHECK_NAME"] = check_name
        start_time = datetime.now(timezone.utc)
        try:
            jr = JobReport.create_dummy(status="error", job_skipped=False)
            jr.dump()
            exit_code = _run_test(check_name, args.run_command)
            job_report = JobReport.load() if JobReport.exist() else None
            assert (
                job_report
            ), "BUG. There must be job report either real report, or pre-report if job was killed"
            job_report.exit_code = exit_code
            job_report.dump()
        except Exception:
            traceback.print_exc()
            print("Run failed")

        # post
        try:
            if JobReport.load().dummy:
                print("ERROR: Job was killed - generate evidence")
                job_report.duration = (
                    start_time - datetime.now(timezone.utc)
                ).total_seconds()
                if Utils.is_killed_with_oom():
                    print("WARNING: OOM while job execution")
                    print(subprocess.run("sudo dmesg -T", check=False))
                    error_description = (
                        f"Out Of Memory, exit_code {job_report.exit_code}"
                    )
                else:
                    error_description = f"Unknown, exit_code {job_report.exit_code}"
                CIBuddy().post_job_error(
                    error_description + f" after {int(job_report.duration)}s",
                    job_name=_get_ext_check_name(args.job_name),
                )
        except Exception:
            traceback.print_exc()
            print("Post failed")
    ### RUN FROM PRAKTIKA action: end

    ### print results
    _print_results(result, args.outfile, args.pretty)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
