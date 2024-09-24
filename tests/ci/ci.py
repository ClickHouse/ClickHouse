import argparse
import concurrent.futures
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import docker_images_helper
import upload_result_helper
from build_check import get_release_or_pr
from ci_buddy import CIBuddy
from ci_cache import CiCache
from ci_config import CI
from ci_metadata import CiMetadata
from ci_settings import CiSettings
from ci_utils import GH, Envs, Utils
from clickhouse_helper import (
    CiLogsCredentials,
    ClickHouseHelper,
    InsertException,
    get_instance_id,
    get_instance_type,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    CommitStatusData,
    RerunHelper,
    format_description,
    get_commit,
    get_commit_filtered_statuses,
    post_commit_status,
    set_status_comment,
)
from digest_helper import DockerDigester
from env_helper import GITHUB_REPOSITORY, GITHUB_RUN_ID, IS_CI, REPO_COPY, TEMP_PATH
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, Git
from git_helper import Runner as GitRunner
from github_helper import GitHub
from pr_info import PRInfo
from report import (
    ERROR,
    FAIL,
    GITHUB_JOB_API_URL,
    JOB_FINISHED_TEST_NAME,
    JOB_STARTED_TEST_NAME,
    OK,
    PENDING,
    SUCCESS,
    BuildResult,
    JobReport,
    TestResult,
)
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from version_helper import get_version_from_repo

# pylint: disable=too-many-lines


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

    if not to_be_skipped:
        print("push start record to ci db")
        prepared_events = prepare_tests_results_for_clickhouse(
            pr_info,
            [TestResult(JOB_STARTED_TEST_NAME, OK)],
            SUCCESS,
            0.0,
            JobReport.get_start_time_from_current(),
            "",
            _get_ext_check_name(job_name),
        )
        ClickHouseHelper().insert_events_into(
            db="default", table="checks", events=prepared_events
        )
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

    if job_config.run_by_label or not job_config.has_digest():
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
            job_config = CI.get_job_config(job)
            if not job_config:
                # there might be a new job that does not exist on this branch - skip it
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
    for retry in range(2):
        try:
            set_status_comment(commit, pr_info)
            break
        except Exception as e:
            print(
                f"WARNING: Failed to update CI Running status, attempt [{retry + 1}], exception [{e}]"
            )
            time.sleep(1)
    else:
        print("ERROR: All retry attempts failed.")
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


def _upload_build_artifacts(
    pr_info: PRInfo,
    build_name: str,
    ci_cache: CiCache,
    job_report: JobReport,
    s3: S3Helper,
    s3_destination: str,
    upload_binary: bool,
) -> str:
    # There are ugly artifacts for the performance test. FIXME:
    s3_performance_path = "/".join(
        (
            get_release_or_pr(pr_info, get_version_from_repo())[1],
            pr_info.sha,
            Utils.normalize_string(build_name),
            "performance.tar.zst",
        )
    )
    performance_urls = []
    assert job_report.build_dir_for_upload, "Must be set for build job"
    performance_path = Path(job_report.build_dir_for_upload) / "performance.tar.zst"
    if upload_binary:
        if performance_path.exists():
            performance_urls.append(
                s3.upload_build_file_to_s3(performance_path, s3_performance_path)
            )
            print(
                "Uploaded performance.tar.zst to %s, now delete to avoid duplication",
                performance_urls[0],
            )
            performance_path.unlink()
        build_urls = (
            s3.upload_build_directory_to_s3(
                Path(job_report.build_dir_for_upload),
                s3_destination,
                keep_dirs_in_s3_path=False,
                upload_symlinks=False,
            )
            + performance_urls
        )
        print("::notice ::Build URLs: {}".format("\n".join(build_urls)))
    else:
        build_urls = []
        print("::notice ::No binaries will be uploaded for this job")
    log_path = Path(job_report.additional_files[0])
    log_url = ""
    if log_path.exists():
        log_url = s3.upload_build_file_to_s3(
            log_path, s3_destination + "/" + log_path.name
        )
    print(f"::notice ::Log URL: {log_url}")

    # generate and upload a build report
    build_result = BuildResult(
        build_name,
        log_url,
        build_urls,
        job_report.version,
        job_report.status,
        int(job_report.duration),
        GITHUB_JOB_API_URL(),
        head_ref=pr_info.head_ref,
        # PRInfo fetches pr number for release branches as well - set pr_number to 0 for release
        #   so that build results are not mistakenly treated as feature branch builds
        pr_number=pr_info.number if pr_info.is_pr else 0,
    )
    report_url = ci_cache.upload_build_report(build_result)
    print(f"Report file has been uploaded to [{report_url}]")

    # Upload master head's binaries
    static_bin_name = CI.get_build_config(build_name).static_binary_name
    if pr_info.is_master and static_bin_name:
        # Full binary with debug info:
        s3_path_full = "/".join((pr_info.base_ref, static_bin_name, "clickhouse-full"))
        binary_full = Path(job_report.build_dir_for_upload) / "clickhouse"
        url_full = s3.upload_build_file_to_s3(binary_full, s3_path_full)
        print(f"::notice ::Binary static URL (with debug info): {url_full}")

        # Stripped binary without debug info:
        s3_path_compact = "/".join((pr_info.base_ref, static_bin_name, "clickhouse"))
        binary_compact = Path(job_report.build_dir_for_upload) / "clickhouse-stripped"
        url_compact = s3.upload_build_file_to_s3(binary_compact, s3_path_compact)
        print(f"::notice ::Binary static URL (compact): {url_compact}")

    return log_url


def _upload_build_profile_data(
    pr_info: PRInfo,
    build_name: str,
    job_report: JobReport,
    git_runner: GitRunner,
    ch_helper: ClickHouseHelper,
) -> None:
    ci_logs_credentials = CiLogsCredentials(Path("/dev/null"))
    if not ci_logs_credentials.host:
        logging.info("Unknown CI logs host, skip uploading build profile data")
        return

    if not pr_info.number == 0:
        logging.info("Skipping uploading build profile data for PRs")
        return

    instance_type = get_instance_type()
    instance_id = get_instance_id()
    auth = {
        "X-ClickHouse-User": "ci",
        "X-ClickHouse-Key": ci_logs_credentials.password,
    }
    url = f"https://{ci_logs_credentials.host}/"
    profiles_dir = Path(TEMP_PATH) / "profiles_source"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    print(
        "Processing profile JSON files from %s",
        Path(REPO_COPY) / "build_docker",
    )
    git_runner(
        "./utils/prepare-time-trace/prepare-time-trace.sh "
        f"build_docker {profiles_dir.absolute()}"
    )
    profile_data_file = Path(TEMP_PATH) / "profile.json"
    with open(profile_data_file, "wb") as profile_fd:
        for profile_source in profiles_dir.iterdir():
            if profile_source.name not in (
                "binary_sizes.txt",
                "binary_symbols.txt",
            ):
                with open(profiles_dir / profile_source, "rb") as ps_fd:
                    profile_fd.write(ps_fd.read())

    @dataclass
    class FileQuery:
        file: Path
        query: str

    profile_query = f"""INSERT INTO build_time_trace
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        library,
        time,
        pid,
        tid,
        ph,
        ts,
        dur,
        cat,
        name,
        detail,
        count,
        avgMs,
        args_name
    )
    SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', *
    FROM input('
        file String,
        library String,
        time DateTime64(6),
        pid UInt32,
        tid UInt32,
        ph String,
        ts UInt64,
        dur UInt64,
        cat String,
        name String,
        detail String,
        count UInt64,
        avgMs UInt64,
        args_name String')
    FORMAT JSONCompactEachRow"""
    binary_sizes_query = f"""INSERT INTO binary_sizes
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        size
    )
    SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', file, size
    FROM input('size UInt64, file String')
    SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
    FORMAT Regexp"""
    binary_symbols_query = f"""INSERT INTO binary_symbols
    (
        pull_request_number,
        commit_sha,
        check_start_time,
        check_name,
        instance_type,
        instance_id,
        file,
        address,
        size,
        type,
        symbol
    )
    SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}',
    file, reinterpretAsUInt64(reverse(unhex(address))), reinterpretAsUInt64(reverse(unhex(size))), type, symbol
    FROM input('file String, address String, size String, type String, symbol String')
    SETTINGS format_regexp = '^([^ ]+) ([0-9a-fA-F]+)(?: ([0-9a-fA-F]+))? (.) (.+)$'
    FORMAT Regexp"""

    files_queries = (
        FileQuery(
            profile_data_file,
            profile_query,
        ),
        FileQuery(
            profiles_dir / "binary_sizes.txt",
            binary_sizes_query,
        ),
        FileQuery(
            profiles_dir / "binary_symbols.txt",
            binary_symbols_query,
        ),
    )
    for fq in files_queries:
        logging.info(
            "Uploading profile data, path: %s, size: %s, query:\n%s",
            fq.file,
            fq.file.stat().st_size,
            fq.query,
        )
        try:
            ch_helper.insert_file(url, auth, fq.query, fq.file, timeout=5)
        except InsertException:
            logging.error("Failed to insert profile data for the build, continue")


def _add_build_to_version_history(
    pr_info: PRInfo,
    job_report: JobReport,
    version: str,
    docker_tag: str,
    ch_helper: ClickHouseHelper,
) -> None:
    # with some probability we will not silently break this logic
    assert pr_info.sha and pr_info.commit_html_url and pr_info.head_ref and version

    data = {
        "check_start_time": job_report.start_time,
        "pull_request_number": pr_info.number,
        "pull_request_url": pr_info.pr_html_url,
        "commit_sha": pr_info.sha,
        "commit_url": pr_info.commit_html_url,
        "version": version,
        "docker_tag": docker_tag,
        "git_ref": pr_info.head_ref,
    }

    print(f"::notice ::Log Adding record to versions history: {data}")

    ch_helper.insert_event_into(db="default", table="version_history", event=data)


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
                    f"ERROR: Run action failed with timeout and did not generate JobReport - update dummy report with execution time"
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
    s3 = S3Helper()
    pr_info = PRInfo()
    git_runner = GitRunner(set_cwd_to_git_root=True)

    ### CONFIGURE action: start
    if args.configure:
        if IS_CI and pr_info.is_pr:
            # store meta on s3 (now we need it only for PRs)
            meta = CiMetadata(s3, pr_info.number, pr_info.head_ref)
            meta.run_id = int(GITHUB_RUN_ID)
            meta.push_meta()

        ci_settings = CiSettings.create_from_pr_message(
            args.commit_message or None, update_from_api=True
        )

        if ci_settings.no_merge_commit and IS_CI:
            git_runner.run(f"{GIT_PREFIX} checkout {pr_info.sha}")

        git_ref = git_runner.run(f"{GIT_PREFIX} rev-parse HEAD")

        # let's get CH version
        version = get_version_from_repo(git=Git(True)).string
        print(f"Got CH version for this commit: [{version}]")

        docker_data = (
            _configure_docker_jobs(args.docker_digest_or_latest)
            if not args.skip_docker
            else {}
        )

        ci_cache = _configure_jobs(
            s3,
            pr_info,
            ci_settings,
            args.skip_jobs,
            args.workflow,
        )

        ci_cache.print_status()
        if IS_CI and pr_info.is_pr and not ci_settings.no_ci_cache:
            ci_cache.filter_out_not_affected_jobs()
            ci_cache.print_status()

        if IS_CI and not pr_info.is_merge_queue:

            if pr_info.is_release and pr_info.is_push_event:
                print("Release/master: CI Cache add pending records for all todo jobs")
                ci_cache.push_pending_all(pr_info.is_release)

            # wait for pending jobs to be finished, await_jobs is a long blocking call
            ci_cache.await_pending_jobs(pr_info.is_release)

        # conclude results
        result["git_ref"] = git_ref
        result["version"] = version
        result["build"] = ci_cache.job_digests[CI.BuildNames.PACKAGE_RELEASE]
        result["docs"] = ci_cache.job_digests[CI.JobNames.DOCS_CHECK]
        result["ci_settings"] = ci_settings.as_dict()
        if not args.skip_jobs:
            result["stages_data"] = _generate_ci_stage_config(
                ci_cache.jobs_to_do, ci_settings.woolen_wolfdog
            )
            result["jobs_data"] = {
                "jobs_to_do": list(ci_cache.jobs_to_do),
                "jobs_to_skip": ci_cache.jobs_to_skip,
                "digests": ci_cache.job_digests,
                "jobs_params": {
                    job: {"batches": config.batches, "num_batches": config.num_batches}
                    for job, config in ci_cache.jobs_to_do.items()
                },
            }
        result["docker_data"] = docker_data
    ### CONFIGURE action: end

    ### PRE action: start
    elif args.pre:
        assert indata, "Run config must be provided via --infile"
        _pre_action(s3, args.job_name, args.batch, indata, pr_info)

    ### RUN action: start
    elif args.run:
        assert indata
        job_report = JobReport.load()
        check_name = args.job_name
        check_name_with_group = _get_ext_check_name(check_name)
        print(
            f"Check if rerun for name: [{check_name}], extended name [{check_name_with_group}]"
        )

        if job_report.job_skipped and not args.force:
            print(
                f"Commit status or Build Report is already present - job will be skipped with status: [{job_report.status}]"
            )
            if job_report.status == SUCCESS:
                exit_code = 0
            else:
                exit_code = 1
        else:
            exit_code = _run_test(check_name, args.run_command)
            job_report = JobReport.load() if JobReport.exist() else None
            assert (
                job_report
            ), "BUG. There must be job report either real report, or pre-report if job was killed"
            job_report.exit_code = exit_code
            job_report.dump()
    ### RUN action: end

    ### POST action: start
    elif args.post:
        job_report = JobReport.load() if JobReport.exist() else None
        assert (
            job_report
        ), "BUG. There must be job report either real report, or pre-report if job was killed"
        error_description = ""
        if not job_report.dummy:
            # it's a real job report
            ch_helper = ClickHouseHelper()
            check_url = ""

            if CI.is_build_job(args.job_name):
                assert (
                    indata
                ), f"--infile with config must be provided for POST action of a build type job [{args.job_name}]"

                # upload binaries only for normal builds in PRs
                upload_binary = (
                    not pr_info.is_pr
                    or CI.get_job_ci_stage(args.job_name) == CI.WorkflowStages.BUILDS_1
                    or CiSettings.create_from_run_config(indata).upload_all
                )

                build_name = args.job_name
                s3_path_prefix = "/".join(
                    (
                        get_release_or_pr(pr_info, get_version_from_repo())[0],
                        pr_info.sha,
                        build_name,
                    )
                )
                log_url = _upload_build_artifacts(
                    pr_info,
                    build_name,
                    ci_cache=CiCache(s3, indata["jobs_data"]["digests"]),
                    job_report=job_report,
                    s3=s3,
                    s3_destination=s3_path_prefix,
                    upload_binary=upload_binary,
                )
                _upload_build_profile_data(
                    pr_info, build_name, job_report, git_runner, ch_helper
                )
                check_url = log_url
            else:
                # test job
                gh = GitHub(get_best_robot_token(), per_page=100)
                additional_urls = []
                s3_path_prefix = "/".join(
                    (
                        get_release_or_pr(pr_info, get_version_from_repo())[0],
                        pr_info.sha,
                        Utils.normalize_string(
                            job_report.check_name or _get_ext_check_name(args.job_name)
                        ),
                    )
                )
                if job_report.build_dir_for_upload:
                    additional_urls = s3.upload_build_directory_to_s3(
                        Path(job_report.build_dir_for_upload),
                        s3_path_prefix,
                        keep_dirs_in_s3_path=False,
                        upload_symlinks=False,
                    )
                if job_report.test_results or job_report.additional_files:
                    check_url = upload_result_helper.upload_results(
                        s3,
                        pr_info.number,
                        pr_info.sha,
                        job_report.test_results,
                        job_report.additional_files,
                        job_report.check_name or _get_ext_check_name(args.job_name),
                        additional_urls=additional_urls or None,
                    )
                commit = get_commit(gh, pr_info.sha)
                post_commit_status(
                    commit,
                    job_report.status,
                    check_url,
                    format_description(job_report.description),
                    job_report.check_name or _get_ext_check_name(args.job_name),
                    pr_info,
                    dump_to_file=True,
                )
            print(f"Job report url: [{check_url}]")
            prepared_events = prepare_tests_results_for_clickhouse(
                pr_info,
                job_report.test_results,
                job_report.status,
                job_report.duration,
                job_report.start_time,
                check_url or "",
                job_report.check_name or _get_ext_check_name(args.job_name),
            )
            ch_helper.insert_events_into(
                db="default", table="checks", events=prepared_events
            )

            if "DockerServerImage" in args.job_name and indata is not None:
                _add_build_to_version_history(
                    pr_info,
                    job_report,
                    indata["version"],
                    indata["build"],
                    ch_helper,
                )
        elif job_report.job_skipped:
            print(f"Skipped after rerun check {[args.job_name]} - do nothing")
        else:
            print(f"ERROR: Job was killed - generate evidence")
            job_report.update_duration()
            ret_code = os.getenv("JOB_EXIT_CODE", "")
            if ret_code:
                try:
                    job_report.exit_code = int(ret_code)
                except ValueError:
                    pass
            if Utils.is_killed_with_oom():
                print("WARNING: OOM while job execution")
                print(subprocess.run("sudo dmesg -T", check=False))
                error_description = f"Out Of Memory, exit_code {job_report.exit_code}"
            else:
                error_description = f"Unknown, exit_code {job_report.exit_code}"
            CIBuddy().post_job_error(
                error_description + f" after {int(job_report.duration)}s",
                job_name=_get_ext_check_name(args.job_name),
            )
            if CI.is_test_job(args.job_name):
                gh = GitHub(get_best_robot_token(), per_page=100)
                commit = get_commit(gh, pr_info.sha)
                check_url = ""
                if job_report.test_results or job_report.additional_files:
                    check_url = upload_result_helper.upload_results(
                        s3,
                        pr_info.number,
                        pr_info.sha,
                        job_report.test_results,
                        job_report.additional_files,
                        job_report.check_name or _get_ext_check_name(args.job_name),
                    )
                post_commit_status(
                    commit,
                    ERROR,
                    check_url,
                    "Error: " + error_description,
                    _get_ext_check_name(args.job_name),
                    pr_info,
                    dump_to_file=True,
                )

        if not job_report.job_skipped:
            print("push finish record to ci db")
            prepared_events = prepare_tests_results_for_clickhouse(
                pr_info,
                [
                    TestResult(
                        JOB_FINISHED_TEST_NAME,
                        FAIL if error_description else OK,
                        raw_logs=error_description or None,
                    )
                ],
                SUCCESS if not error_description else ERROR,
                0.0,
                JobReport.get_start_time_from_current(),
                "",
                _get_ext_check_name(args.job_name),
            )
            ClickHouseHelper().insert_events_into(
                db="default", table="checks", events=prepared_events
            )
    ### POST action: end

    ### MARK SUCCESS action: start
    elif args.mark_success:
        assert indata, "Run config must be provided via --infile"
        _mark_success_action(s3, indata, pr_info, args.job_name, args.batch)

    ### UPDATE GH STATUSES action: start
    elif args.update_gh_statuses:
        assert indata, "Run config must be provided via --infile"
        _update_gh_statuses_action(indata=indata, s3=s3)

    ### CANCEL THE PREVIOUS WORKFLOW RUN
    elif args.cancel_previous_run:
        if pr_info.is_merge_queue:
            _cancel_pr_workflow(s3, pr_info.merged_pr)
        elif pr_info.is_pr:
            _cancel_pr_workflow(s3, pr_info.number, cancel_sync=True)
        else:
            assert False, "BUG! Not supported scenario"

    ### SET PENDING STATUS
    elif args.set_pending_status:
        if pr_info.is_pr:
            _set_pending_statuses(pr_info)
        else:
            assert False, "BUG! Not supported scenario"

    ### print results
    _print_results(result, args.outfile, args.pretty)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
