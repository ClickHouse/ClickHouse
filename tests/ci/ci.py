import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import docker_images_helper
from ci_config import CI_CONFIG
from commit_status_helper import (
    CommitStatusData,
    format_description,
    get_commit,
    set_status_comment,
)
from digest_helper import DockerDigester, JobDigester
from env_helper import CI, REPORT_PATH, ROOT_DIR, S3_BUILDS_BUCKET, TEMP_PATH
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, Git
from git_helper import Runner as GitRunner
from github import Github
from pr_info import PRInfo
from report import BuildResult
from s3_helper import S3Helper
from version_helper import get_version_from_repo


def get_check_name(check_name: str, batch: int, num_batches: int) -> str:
    res = check_name
    if num_batches > 1:
        res = f"{check_name} [{batch+1}/{num_batches}]"
    return res


def normalize_check_name(check_name: str) -> str:
    res = check_name.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_")):
        res = res.replace(*r)
    return res


def is_build_job(job: str) -> bool:
    if "package_" in job or "binary_" in job or job == "fuzzers":
        return True
    return False


def is_test_job(job: str) -> bool:
    return not is_build_job(job) and not "Style" in job and not "Docs check" in job


def is_docs_job(job: str) -> bool:
    return "Docs check" in job


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    # FIXME: consider switching to sub_parser for configure, pre, run, post actions
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Action that configures ci run. Calculates digests, checks job to be executed, generates json output",
    )
    parser.add_argument(
        "--update-gh-statuses",
        action="store_true",
        help="Action that recreate success GH statuses for jobs that finished successfully in past and will be skipped this time",
    )
    parser.add_argument(
        "--pre",
        action="store_true",
        help="Action that executes prerequesetes for the job provided in --job-name",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given job name.",
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
        help="skip fetching data about job runs, used in --configure action (for debugging and nigthly ci)",
    )
    parser.add_argument(
        "--rebuild-all-docker",
        action="store_true",
        default=False,
        help="will create run config for rebuilding all dockers, used in --configure action (for nightly docker job)",
    )
    parser.add_argument(
        "--rebuild-all-binaries",
        action="store_true",
        default=False,
        help="will create run config without skipping build jobs in any case, used in --configure action (for release branches)",
    )
    return parser.parse_args()


def get_file_flag_name(
    job_name: str, digest: str, batch: int = 0, num_batches: int = 1
) -> str:
    if num_batches < 2:
        return f"job_{job_name}_{digest}.ci"
    else:
        return f"job_{job_name}_{digest}_{batch}_{num_batches}.ci"


def get_s3_path(build_digest: str) -> str:
    return f"CI_data/BUILD-{build_digest}/"


def get_s3_path_docs(digest: str) -> str:
    return f"CI_data/DOCS-{digest}/"


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


def _check_and_update_for_early_style_check(run_config: dict) -> None:
    """
    This is temporary hack to start style check before docker build if possible
    FIXME: need better solution to do style check as soon as possible and as fast as possible w/o dependency on docker job
    """
    jobs_to_do = run_config.get("jobs_data", {}).get("jobs_to_do", [])
    docker_to_build = run_config.get("docker_data", {}).get("missing_multi", [])
    if (
        "Style check" in jobs_to_do
        and docker_to_build
        and "clickhouse/style-test" not in docker_to_build
    ):
        index = jobs_to_do.index("Style check")
        jobs_to_do[index] = "Style check early"


def _update_config_for_docs_only(run_config: dict) -> None:
    DOCS_CHECK_JOBS = ["Docs check", "Style check"]
    print(f"NOTE: Will keep only docs related jobs: [{DOCS_CHECK_JOBS}]")
    jobs_to_do = run_config.get("jobs_data", {}).get("jobs_to_do", [])
    run_config["jobs_data"]["jobs_to_do"] = [
        job for job in jobs_to_do if job in DOCS_CHECK_JOBS
    ]


def _configure_docker_jobs(
    rebuild_all_dockers: bool, docker_digest_or_latest: bool = False
) -> Dict:
    # generate docker jobs data
    docker_digester = DockerDigester()
    imagename_digest_dict = (
        docker_digester.get_all_digests()
    )  # 'image name - digest' mapping
    images_info = docker_images_helper.get_images_info()

    # a. check missing images
    if not rebuild_all_dockers:
        # FIXME: we need login as docker manifest inspect goes directly to one of the *.docker.com hosts instead of "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
        #         find if it's possible to use the setting of /etc/docker/daemon.json
        docker_images_helper.docker_login()
        print("Start checking missing images in dockerhub")
        missing_multi_dict = check_missing_images_on_dockerhub(imagename_digest_dict)
        missing_multi = list(missing_multi_dict)
        missing_amd64 = []
        missing_aarch64 = []
        if not docker_digest_or_latest:
            # look for missing arm and amd images only among missing multiarch manifests @missing_multi_dict
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
        # FIXME: temporary hack, remove after transition to docker digest as tag
        else:
            if missing_multi:
                print(
                    f"WARNING: Missing images {list(missing_multi)} - fallback to latest tag"
                )
                for image in missing_multi:
                    imagename_digest_dict[image] = "latest"
        print("...checking missing images in dockerhub - done")
    else:
        # add all images to missing
        missing_multi = list(imagename_digest_dict)
        missing_amd64 = missing_multi
        # FIXME: WA until full arm support: skip not supported arm images
        missing_aarch64 = [
            name
            for name in imagename_digest_dict
            if not images_info[name]["only_amd64"]
        ]

    return {
        "images": imagename_digest_dict,
        "missing_aarch64": missing_aarch64,
        "missing_amd64": missing_amd64,
        "missing_multi": missing_multi,
    }


def _configure_jobs(
    build_digest: str,
    docs_digest: str,
    job_digester: JobDigester,
    s3: S3Helper,
    rebuild_all_binaries: bool,
    pr_labels: Iterable[str],
    commit_tokens: List[str],
) -> Dict:
    # a. digest each item from the config
    job_digester = JobDigester()
    jobs_params: Dict[str, Dict] = {}
    jobs_to_do: List[str] = []
    jobs_to_skip: List[str] = []
    digests: Dict[str, str] = {}
    print("Calculating job digests - start")
    for job in CI_CONFIG.job_generator():
        digest = job_digester.get_job_digest(CI_CONFIG.get_digest_config(job))
        digests[job] = digest
        print(f"    job [{job.rjust(50)}] has digest [{digest}]")
    print("Calculating job digests - done")

    # b. check if we have something done
    path = get_s3_path(build_digest)
    done_files = s3.list_prefix(path)
    done_files = [file.split("/")[-1] for file in done_files]
    print(f"S3 CI files for the build [{build_digest}]: {done_files}")
    docs_path = get_s3_path_docs(docs_digest)
    done_files_docs = s3.list_prefix(docs_path)
    done_files_docs = [file.split("/")[-1] for file in done_files_docs]
    print(f"S3 CI files for the docs [{docs_digest}]: {done_files_docs}")
    done_files += done_files_docs
    for job in digests:
        digest = digests[job]
        job_config = CI_CONFIG.get_job_config(job)
        num_batches: int = job_config.num_batches
        batches_to_do: List[int] = []

        if job_config.run_by_label:
            # this job controlled by label, add to todo if it's labe is set in pr
            if job_config.run_by_label in pr_labels:
                for batch in range(num_batches):  # type: ignore
                    batches_to_do.append(batch)
        elif job_config.run_always:
            # always add to todo
            batches_to_do.append(batch)
        else:
            # this job controlled by digest, add to todo if it's not successfully done before
            for batch in range(num_batches):  # type: ignore
                success_flag_name = get_file_flag_name(job, digest, batch, num_batches)
                if success_flag_name not in done_files or (
                    rebuild_all_binaries and is_build_job(job)
                ):
                    batches_to_do.append(batch)

        if batches_to_do:
            jobs_to_do.append(job)
            jobs_params[job] = {
                "batches": batches_to_do,
                "num_batches": num_batches,
            }
        else:
            jobs_to_skip += (job,)

    if pr_labels:
        jobs_requested_by_label = []  # type: List[str]
        ci_controlling_labels = []  # type: List[str]
        for label in pr_labels:
            label_config = CI_CONFIG.get_label_config(label)
            if label_config:
                jobs_requested_by_label += label_config.run_jobs
                ci_controlling_labels += [label]
        if ci_controlling_labels:
            print(f"NOTE: CI controlling labels are set: [{ci_controlling_labels}]")
            print(
                f"    :   following jobs will be executed: [{jobs_requested_by_label}]"
            )
            jobs_to_do = jobs_requested_by_label

    if commit_tokens:
        requested_jobs = [
            token[len("#job_") :]
            for token in commit_tokens
            if token.startswith("#job_")
        ]
        if requested_jobs:
            assert any(
                len(x) > 1 for x in requested_jobs
            ), f"Invalid job names requested [{requested_jobs}]"
            jobs_to_do_requested = []
            for job in requested_jobs:
                job_with_parents = CI_CONFIG.get_job_with_parents(job)
                # always add requested job itself, even if it could be skipped
                jobs_to_do_requested.append(job_with_parents[0])
                for parent in job_with_parents[1:]:
                    if parent in jobs_to_do and parent not in jobs_to_do_requested:
                        jobs_to_do_requested.append(parent)
            print(
                f"NOTE: Only specific job(s) were requested by commit message tokens: [{jobs_to_do_requested}]"
            )
            jobs_to_do = jobs_to_do_requested

    return {
        "digests": digests,
        "jobs_to_do": jobs_to_do,
        "jobs_to_skip": jobs_to_skip,
        "jobs_params": jobs_params,
    }


def _update_gh_statuses(indata: Dict, s3: S3Helper) -> None:
    # This action is required to re-create all GH statuses for skipped jobs, so that ci report can be generated afterwards
    temp_path = Path(TEMP_PATH)
    if not temp_path.exists():
        temp_path.mkdir(parents=True, exist_ok=True)

    # clean up before start
    for file in temp_path.glob("*.ci"):
        file.unlink()

    # download all metadata files
    path = get_s3_path(indata["build"])
    files = s3.download_files(  # type: ignore
        bucket=S3_BUILDS_BUCKET,
        s3_path=path,
        file_suffix=".ci",
        local_directory=temp_path,
    )
    print(f"CI metadata files [{files}]")
    path = get_s3_path_docs(indata["docs"])
    files_docs = s3.download_files(  # type: ignore
        bucket=S3_BUILDS_BUCKET,
        s3_path=path,
        file_suffix=".ci",
        local_directory=temp_path,
    )
    print(f"CI docs metadata files [{files_docs}]")
    files += files_docs

    # parse CI metadata
    job_digests = indata["jobs_data"]["digests"]
    # create GH status
    pr_info = PRInfo()
    commit = get_commit(Github(get_best_robot_token(), per_page=100), pr_info.sha)

    def run_create_status(job, digest, batch, num_batches):
        success_flag_name = get_file_flag_name(job, digest, batch, num_batches)
        if success_flag_name in files:
            print(f"Going to re-create GH status for job [{job}] sha [{pr_info.sha}]")
            job_status = CommitStatusData.load_from_file(
                f"{TEMP_PATH}/{success_flag_name}"
            )  # type: CommitStatusData
            assert job_status.status == "success", "BUG!"
            commit.create_status(
                state=job_status.status,
                target_url=job_status.report_url,
                description=format_description(
                    f"Reused from [{job_status.pr_num}-{job_status.sha[0:8]}]: "
                    f"{job_status.description}"
                ),
                context=get_check_name(job, batch=batch, num_batches=num_batches),
            )
            print(f"GH status re-created from file [{success_flag_name}]")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for job in job_digests:
            if is_build_job(job):
                # no GH status for build jobs
                continue
            digest = job_digests[job]
            num_batches = CI_CONFIG.get_job_config(job).num_batches
            for batch in range(num_batches):
                future = executor.submit(
                    run_create_status, job, digest, batch, num_batches
                )
                futures.append(future)
        done, _ = concurrent.futures.wait(futures)
        for future in done:
            try:
                _ = future.result()
            except Exception as e:
                raise e
    print("Going to update overall CI report")
    set_status_comment(commit, pr_info)
    print("... CI report update - done")

    # clean up
    ci_files = list(temp_path.glob("*.ci"))
    for file in ci_files:
        file.unlink()


def _fetch_commit_tokens(message: str) -> List[str]:
    pattern = r"#[\w-]+"
    matches = re.findall(pattern, message)
    res = [
        match
        for match in matches
        if match == "#no-merge-commit"
        or match.startswith("#job_")
        or match.startswith("#job-")
    ]
    return res


def main() -> int:
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if args.mark_success or args.pre or args.post or args.run:
        assert args.infile, "Run config must be provided via --infile"
        assert args.job_name, "Job name must be provided via --job-name"

    indata: Optional[Dict[str, Any]] = None
    if args.infile:
        indata = (
            json.loads(args.infile)
            if not os.path.isfile(args.infile)
            else json.load(open(args.infile))
        )
        assert indata and isinstance(indata, dict), "Invalid --infile json"

    result: Dict[str, Any] = {}
    s3 = S3Helper()

    if args.configure:
        GR = GitRunner()
        pr_info = PRInfo()

        docker_data = {}
        git_ref = GR.run(f"{GIT_PREFIX} rev-parse HEAD")

        # if '#no-merge-commit' is set in commit message - set git ref to PR branch head to avoid merge-commit
        tokens = []
        if pr_info.number != 0 and not args.skip_jobs:
            message = GR.run(f"{GIT_PREFIX} log {pr_info.sha} --format=%B -n 1")
            tokens = _fetch_commit_tokens(message)
            print(f"Found commit message tokens: [{tokens}]")
            if "#no-merge-commit" in tokens and CI:
                GR.run(f"{GIT_PREFIX} checkout {pr_info.sha}")
                git_ref = GR.run(f"{GIT_PREFIX} rev-parse HEAD")
                print(
                    "#no-merge-commit is set in commit message - Setting git ref to PR branch HEAD to not use merge commit"
                )

        # let's get CH version
        version = get_version_from_repo(git=Git(True)).string
        print(f"Got CH version for this commit: [{version}]")

        docker_data = (
            _configure_docker_jobs(
                args.rebuild_all_docker, args.docker_digest_or_latest
            )
            if not args.skip_docker
            else {}
        )

        job_digester = JobDigester()
        build_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config("package_release")
        )
        docs_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config("Docs check")
        )
        jobs_data = (
            _configure_jobs(
                build_digest,
                docs_digest,
                job_digester,
                s3,
                args.rebuild_all_binaries,
                pr_info.labels,
                tokens,
            )
            if not args.skip_jobs
            else {}
        )

        # conclude results
        result["git_ref"] = git_ref
        result["version"] = version
        result["build"] = build_digest
        result["docs"] = docs_digest
        result["jobs_data"] = jobs_data
        result["docker_data"] = docker_data
        if pr_info.number != 0 and not args.docker_digest_or_latest:
            # FIXME: it runs style check before docker build if possible (style-check images is not changed)
            #    find a way to do style check always before docker build and others
            _check_and_update_for_early_style_check(result)
        if pr_info.has_changes_in_documentation_only():
            _update_config_for_docs_only(result)

    elif args.update_gh_statuses:
        assert indata, "Run config must be provided via --infile"
        _update_gh_statuses(indata=indata, s3=s3)

    elif args.pre:
        # remove job status file if any
        CommitStatusData.cleanup()

        if is_test_job(args.job_name):
            assert indata, "Run config must be provided via --infile"
            report_path = Path(REPORT_PATH)
            report_path.mkdir(exist_ok=True, parents=True)
            path = get_s3_path(indata["build"])
            files = s3.download_files(  # type: ignore
                bucket=S3_BUILDS_BUCKET,
                s3_path=path,
                file_suffix=".json",
                local_directory=report_path,
            )
            print(
                f"Pre action done. Report files [{files}] have been downloaded from [{path}] to [{report_path}]"
            )
        else:
            print(f"Pre action done. Nothing to do for [{args.job_name}]")

    elif args.run:
        assert CI_CONFIG.get_job_config(
            args.job_name
        ).run_command, f"Run command must be configured in CI_CONFIG for [{args.job_name}] or in GH workflow"
        if CI_CONFIG.get_job_config(args.job_name).timeout:
            os.environ["KILL_TIMEOUT"] = str(
                CI_CONFIG.get_job_config(args.job_name).timeout
            )
        os.environ["CHECK_NAME"] = args.job_name
        run_command = (
            "./tests/ci/" + CI_CONFIG.get_job_config(args.job_name).run_command
        )
        if ".py" in run_command:
            run_command = "python3 " + run_command
        print(f"Going to start run command [{run_command}]")
        process = subprocess.run(
            run_command,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            check=False,
            shell=True,
        )
        if process.returncode == 0:
            print(f"Run action done for: [{args.job_name}]")
        else:
            print(
                f"Run action failed for: [{args.job_name}] with exit code [{process.returncode}]"
            )
            exit_code = process.returncode

    elif args.post:
        if is_build_job(args.job_name):
            report_path = Path(TEMP_PATH)  # build-check.py stores report in TEMP_PATH
            assert report_path.is_dir(), f"File [{report_path}] is not a dir"
            files = list(report_path.glob(f"*{args.job_name}.json"))  # type: ignore[arg-type]
            assert len(files) == 1, f"Which is the report file: {files}?"
            local_report = f"{files[0]}"
            report_name = BuildResult.get_report_name(args.job_name)
            assert indata
            s3_path = Path(get_s3_path(indata["build"])) / report_name
            report_url = s3.upload_file(
                bucket=S3_BUILDS_BUCKET, file_path=local_report, s3_path=s3_path
            )
            print(
                f"Post action done. Report file [{local_report}] has been uploaded to [{report_url}]"
            )
        else:
            print(f"Post action done. Nothing to do for [{args.job_name}]")

    elif args.mark_success:
        assert indata, "Run config must be provided via --infile"
        job = args.job_name
        job_config = CI_CONFIG.get_job_config(job)
        num_batches = job_config.num_batches
        assert (
            num_batches <= 1 or 0 <= args.batch < num_batches
        ), f"--batch must be provided and in range [0, {num_batches}) for {job}"

        # FIXME: find generic design for propagating and handling job status (e.g. stop using statuses in GH api)
        #   now job ca be build job w/o status data, any other job that exit with 0 with or w/o status data
        if is_build_job(job):
            # there is no status for build jobs
            # create dummy success to mark it as done
            job_status = CommitStatusData(
                status="success", description="dummy status", report_url="dummy_url"
            )
        else:
            if not CommitStatusData.is_present():
                # apparently exit after rerun-helper check
                # do nothing, exit without failure
                print(f"ERROR: no status file for job [{job}]")
                job_status = CommitStatusData(
                    status="dummy failure",
                    description="dummy status",
                    report_url="dummy_url",
                )
            else:
                # normal case
                job_status = CommitStatusData.load_status()

        # Storing job data (report_url) to restore OK GH status on job results reuse
        if job_config.run_always:
            print(f"Job [{job}] runs always in CI - do not mark as done")
        elif job_status.is_ok():
            success_flag_name = get_file_flag_name(
                job, indata["jobs_data"]["digests"][job], args.batch, num_batches
            )
            if not is_docs_job(job):
                path = get_s3_path(indata["build"]) + success_flag_name
            else:
                path = get_s3_path_docs(indata["docs"]) + success_flag_name
            job_status.dump_to_file(success_flag_name)
            _ = s3.upload_file(
                bucket=S3_BUILDS_BUCKET, file_path=success_flag_name, s3_path=path
            )
            os.remove(success_flag_name)
            print(
                f"Job [{job}] with digest [{indata['jobs_data']['digests'][job]}] {f'and batch {args.batch}/{num_batches}' if num_batches > 1 else ''} marked as successful. path: [{path}]"
            )
        else:
            print(f"Job [{job}] is not ok, status [{job_status.status}]")

    # print results
    if args.outfile:
        with open(args.outfile, "w") as f:
            if isinstance(result, str):
                print(result, file=f)
            elif isinstance(result, dict):
                print(json.dumps(result, indent=2 if args.pretty else None), file=f)
            else:
                raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    else:
        if isinstance(result, str):
            print(result)
        elif isinstance(result, dict):
            print(json.dumps(result, indent=2 if args.pretty else None))
        else:
            raise AssertionError(f"Unexpected type for 'res': {type(result)}")

    return exit_code


if __name__ == "__main__":
    os.chdir(ROOT_DIR)
    sys.exit(main())
