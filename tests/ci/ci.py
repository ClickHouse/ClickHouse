import argparse
import json
import os
import concurrent.futures
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Optional

from github import Github
from s3_helper import S3Helper
from digest_helper import DockerDigester, JobDigester
import docker_images_helper
from env_helper import (
    ROOT_DIR,
    S3_BUILDS_BUCKET,
    S3_TEST_REPORTS_BUCKET,
    S3_URL,
    TEMP_PATH,
    REPORT_PATH,
)
from commit_status_helper import get_commit, set_status_comment
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from ci_config import CI_CONFIG


class SuccessJobMeta:
    """
    Class object stores data about successfully finished job
    """

    def __init__(self, sha, ref, report_url):
        self.sha = sha
        self.ref = ref  # GITHUB_REF_NAME: master or pr_num
        self.report_url = report_url

    @staticmethod
    def create_job_meta(check_name: str, sha: str, ref: int):  # type: ignore
        pr_num = ref
        report_url = f"{S3_URL}/{S3_TEST_REPORTS_BUCKET}/{pr_num}/{sha}/{normalize_check_name(check_name)}.html"
        return SuccessJobMeta(sha, ref, report_url)

    def dump_to_file(self, file_name: str) -> None:
        with open(file_name, "w") as file:
            print(json.dumps(self.__dict__), file=file)

    @staticmethod
    def create_from_file(file_name: str):  # type: ignore
        return SuccessJobMeta(**json.load(open(file_name)))


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
    if "package_" in job or "binary_" in job:
        return True
    return False


def is_test_job(job: str) -> bool:
    return not is_build_job(job) and not "Style" in job and not "Docs check" in job


def is_docs_job(job: str) -> bool:
    return "Docs check" in job


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
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
        help="Action that executes postrequisites for the job provided in --job-name",
    )
    parser.add_argument(
        "--mark-success",
        action="store_true",
        help="Action that marks job provided in --job-name (with batch provided in --batch) as successfull",
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
        help="otput file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes json output pretty formated",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        default=False,
        help="skip fetching docker data from dockerhub, used in --configure action (for debugging)",
    )
    parser.add_argument(
        "--skip-jobs",
        action="store_true",
        default=False,
        help="skip fetching data about job runs, used in --configure action (for debugging)",
    )
    parser.add_argument(
        "--rebuild-all-docker",
        action="store_true",
        default=False,
        help="will create config for rebuilding all dockers, used in --configure action (for nightly docker job)",
    )
    parser.add_argument(
        "--rebuild-all-binaries",
        action="store_true",
        default=False,
        help="will create config without skipping build jobs in any case , used in --configure action (for release branches)",
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
                    print(f"Eror: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Eror: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Eror: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


def check_and_update_for_early_style_check(run_config: dict) -> None:
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


def main() -> int:
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if (args.mark_success or args.pre or args.post) and not args.infile:
        print("ERROR: need option --infile to be provided")
        parser.print_help()
        parser.exit(1)
    if args.mark_success or args.pre or args.post or args.run:
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
        docker_data = {}
        if not args.skip_docker:
            # generate docker jobs data
            docker_digester = DockerDigester()
            imagename_digest_dict = (
                docker_digester.get_all_digests()
            )  # 'image name - digest' mapping
            images_info = docker_images_helper.get_images_info()

            # a. check missing images
            print("Start checking missing images in dockerhub")
            if not args.rebuild_all_docker:
                missing_multi_dict = check_missing_images_on_dockerhub(
                    imagename_digest_dict
                )

                # look for missing arm and amd images only among missing multiarch manifests @missing_multi_dict
                # to avoid extra dockerhub api calls
                missing_amd64 = list(
                    check_missing_images_on_dockerhub(
                        missing_multi_dict, "amd64"
                    ).keys()
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
                    ).keys()
                )
                missing_multi = list(missing_multi_dict)
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
            print("...checking missing images in dockerhub - done")
            docker_data = {
                "images": imagename_digest_dict,
                "missing_aarch64": missing_aarch64,
                "missing_amd64": missing_amd64,
                "missing_multi": missing_multi,
            }
        # generate jobs data
        jobs_data: Dict[str, Any] = {}
        if not args.skip_jobs:
            # a. digest each item from the config
            job_digester = JobDigester()
            jobs_params: Dict[str, Dict] = {}
            jobs_to_do: List[str] = []
            jobs_to_skip: List[str] = []
            digests: Dict[str, str] = {}
            build_digest = job_digester.get_job_digest(
                CI_CONFIG.get_digest_config("package_release")
            )
            docs_digest = job_digester.get_job_digest(
                CI_CONFIG.get_digest_config("Docs check")
            )
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
                num_batches: int = CI_CONFIG.get_job_config(job).num_batches
                batches_to_do: List[int] = []
                for batch in range(num_batches):  # type: ignore
                    success_flag_name = get_file_flag_name(
                        job, digest, batch, num_batches
                    )
                    if success_flag_name not in done_files or (
                        args.rebuild_all_binaries and is_build_job(job)
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
                jobs_data = {
                    "digests": digests,
                    "jobs_to_do": jobs_to_do,
                    "jobs_to_skip": jobs_to_skip,
                    "jobs_params": jobs_params,
                }
        # conclude results
        if not args.skip_jobs:
            result["build"] = build_digest
            result["docs"] = docs_digest
            result["jobs_data"] = jobs_data
        if not args.skip_docker:
            result["docker_data"] = docker_data
        check_and_update_for_early_style_check(result)
        # END: --configure action
    elif args.update_gh_statuses:
        # This action is required to re-create all GH statuses for skiped jobs, so that ci report can be generated afterwards
        assert indata, "Run config must be provided via --infile"
        temp_path = Path(TEMP_PATH)
        if not temp_path.exists():
            temp_path.mkdir(parents=True, exist_ok=True)

        # clean up before start
        ci_files = list(temp_path.glob("*.ci"))
        for file in ci_files:
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
        for job in job_digests:
            digest = job_digests[job]
            num_batches: int = CI_CONFIG.get_job_config(job).num_batches  # type: ignore[no-redef]
            for batch in range(num_batches):  # type: ignore
                success_flag_name = get_file_flag_name(job, digest, batch, num_batches)
                if success_flag_name in files:
                    if os.path.getsize(f"{TEMP_PATH}/{success_flag_name}") == 0:
                        # FIXME: remove if after transition
                        continue
                    print(f"Going to re-create GH status for job [{job}]")
                    job_meta = SuccessJobMeta.create_from_file(
                        f"{TEMP_PATH}/{success_flag_name}"
                    )
                    commit.create_status(
                        state="success",
                        target_url=job_meta.report_url,
                        description=f"Reused from [{job_meta.ref}-{job_meta.sha[0:8]}]",
                        context=get_check_name(
                            job, batch=batch, num_batches=num_batches
                        ),
                    )
                    print(f"GH status re-created from file [{success_flag_name}]")
        print("Going to update overall CI report")
        set_status_comment(commit, pr_info)
        print("... CI report update - done")

        # clean up
        ci_files = list(temp_path.glob("*.ci"))
        for file in ci_files:
            file.unlink()
        # END: --update-gh-statuses action
    elif args.post:
        if is_build_job(args.job_name):
            assert indata, "Run config must be provided via --infile"
            report_path = Path(TEMP_PATH) # build-check.py stores report in TEMP_PATH
            assert report_path.is_dir(), f"File [{report_path}] is not a dir"
            files = list(report_path.glob(f"*{args.job_name}.json"))  # type: ignore[arg-type]
            assert len(files) == 1, f"Which is the report file: {files}?"
            local_report = f"{files[0]}"
            report_name = f"{args.job_name}.json"
            s3_path = get_s3_path(indata["build"]) + report_name
            report_url = s3.upload_file(
                bucket=S3_BUILDS_BUCKET, file_path=local_report, s3_path=s3_path
            )
            print(
                f"Post action done. Report file [{local_report}] has been uploaded to [{report_url}]"
            )
        else:
            print(f"Post action done. Nothing to do for [{args.job_name}]")
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
            shell=True
        )
        if process.returncode == 0:
            print(f"Run action done for: [{args.job_name}]")
        else:
            print(
                f"Run action failed for: [{args.job_name}] with exit code [{process.returncode}]"
            )
            exit_code = process.returncode
    elif args.pre:
        if is_test_job(args.job_name):
            assert indata, "Run config must be provided via --infile"
            # FIXME: avoid using env
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
            print("Pre action done. Nothing to do for [{args.job_name}]")
    elif args.mark_success:
        assert indata, "Run config must be provided via --infile"
        job = args.job_name
        num_batches = CI_CONFIG.get_job_config(job).num_batches
        assert (
            num_batches <= 1 or 0 <= args.batch < num_batches
        ), f"--batch must be provided and in range [0, {num_batches}) for {job}"
        success_flag_name = get_file_flag_name(
            job, indata["jobs_data"]["digests"][job], args.batch, num_batches
        )
        if not is_docs_job(job):
            path = get_s3_path(indata["build"]) + success_flag_name
        else:
            path = get_s3_path_docs(indata["docs"]) + success_flag_name

        # create success file flag with metadata, upload to s3, remove local file
        SuccessJobMeta.create_job_meta(
            get_check_name(job, args.batch, num_batches), PRInfo().sha, PRInfo().number
        ).dump_to_file(success_flag_name)
        _ = s3.upload_file(
            bucket=S3_BUILDS_BUCKET, file_path=success_flag_name, s3_path=path
        )
        os.remove(success_flag_name)

        print(
            f"Job [{job}] with digest [{indata['jobs_data']['digests'][job]}] {f'and batch {args.batch}/{num_batches}' if num_batches > 1 else ''} marked as successful. path: [{path}]"
        )

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
