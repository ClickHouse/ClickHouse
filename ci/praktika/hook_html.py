import dataclasses
import json
import urllib.parse
from pathlib import Path
from typing import List

from praktika._environment import _Environment
from praktika.gh import GH
from praktika.parser import WorkflowConfigParser
from praktika.result import Result, ResultInfo
from praktika.runtime import RunConfig
from praktika.s3 import S3
from praktika.settings import Settings
from praktika.utils import Utils


@dataclasses.dataclass
class GitCommit:
    # date: str
    # message: str
    sha: str

    @staticmethod
    def from_json(file) -> List["GitCommit"]:
        commits = []
        json_data = None
        try:
            with open(file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            commits = [
                GitCommit(
                    # message=commit["messageHeadline"],
                    sha=commit["sha"],
                    # date=commit["committedDate"],
                )
                for commit in json_data
            ]
        except Exception as e:
            print(
                f"ERROR: Failed to deserialize commit's data [{json_data}], ex: [{e}]"
            )

        return commits

    @classmethod
    def update_s3_data(cls):
        env = _Environment.get()
        sha = env.SHA
        if not sha:
            print("WARNING: Failed to retrieve commit sha")
            return
        commits = cls.pull_from_s3()
        for commit in commits:
            if sha == commit.sha:
                print(
                    f"INFO: Sha already present in commits data [{sha}] - skip data update"
                )
                return
        commits.append(GitCommit(sha=sha))
        cls.push_to_s3(commits)
        return

    @classmethod
    def dump(cls, commits):
        commits_ = []
        for commit in commits:
            commits_.append(dataclasses.asdict(commit))
        with open(cls.file_name(), "w", encoding="utf8") as f:
            json.dump(commits_, f)

    @classmethod
    def pull_from_s3(cls):
        local_path = Path(cls.file_name())
        file_name = local_path.name
        env = _Environment.get()
        s3_path = f"{Settings.HTML_S3_PATH}/{cls.get_s3_prefix(pr_number=env.PR_NUMBER, branch=env.BRANCH)}/{file_name}"
        if not S3.copy_file_from_s3(s3_path=s3_path, local_path=local_path):
            print(f"WARNING: failed to cp file [{s3_path}] from s3")
            return []
        return cls.from_json(local_path)

    @classmethod
    def push_to_s3(cls, commits):
        print(f"INFO: push commits data to s3, commits num [{len(commits)}]")
        cls.dump(commits)
        local_path = Path(cls.file_name())
        file_name = local_path.name
        env = _Environment.get()
        s3_path = f"{Settings.HTML_S3_PATH}/{cls.get_s3_prefix(pr_number=env.PR_NUMBER, branch=env.BRANCH)}/{file_name}"
        if not S3.copy_file_to_s3(s3_path=s3_path, local_path=local_path, text=True):
            print(f"WARNING: failed to cp file [{local_path}] to s3")

    @classmethod
    def get_s3_prefix(cls, pr_number, branch):
        prefix = ""
        assert pr_number or branch
        if pr_number and pr_number > 0:
            prefix += f"{pr_number}"
        else:
            prefix += f"{branch}"
        return prefix

    @classmethod
    def file_name(cls):
        return f"{Settings.TEMP_DIR}/commits.json"

    # def _get_pr_commits(pr_number):
    #     res = []
    #     if not pr_number:
    #         return res
    #     output = Shell.get_output(f"gh pr view {pr_number}  --json commits")
    #     if output:
    #         res = GitCommit.from_json(output)
    #     return res


class HtmlRunnerHooks:
    @classmethod
    def configure(cls, _workflow):
        # generate pending Results for all jobs in the workflow
        if _workflow.enable_cache:
            skip_jobs = RunConfig.from_fs(_workflow.name).cache_success
        else:
            skip_jobs = []

        env = _Environment.get()
        results = []
        for job in _workflow.jobs:
            if job.name not in skip_jobs:
                result = Result.generate_pending(job.name)
            else:
                result = Result.generate_skipped(job.name)
            results.append(result)
        summary_result = Result.generate_pending(_workflow.name, results=results)
        summary_result.aux_links.append(env.CHANGE_URL)
        summary_result.aux_links.append(env.RUN_URL)
        summary_result.start_time = Utils.timestamp()
        page_url = "/".join(
            ["https:/", Settings.HTML_S3_PATH, str(Path(Settings.HTML_PAGE_FILE).name)]
        )
        for bucket, endpoint in Settings.S3_BUCKET_TO_HTTP_ENDPOINT.items():
            page_url = page_url.replace(bucket, endpoint)
        # TODO: add support for non-PRs (use branch?)
        page_url += f"?PR={env.PR_NUMBER}&sha=latest&name_0={urllib.parse.quote(env.WORKFLOW_NAME, safe='')}"
        summary_result.html_link = page_url

        # clean the previous latest results in PR if any
        if env.PR_NUMBER:
            S3.clean_latest_result()
        S3.copy_result_to_s3(
            summary_result,
            unlock=False,
        )

        print(f"CI Status page url [{page_url}]")

        res1 = GH.post_commit_status(
            name=_workflow.name,
            status=Result.Status.PENDING,
            description="",
            url=page_url,
        )
        res2 = GH.post_pr_comment(
            comment_body=f"Workflow [[{_workflow.name}]({page_url})], commit [{_Environment.get().SHA[:8]}]",
            or_update_comment_with_substring=f"Workflow [",
        )
        if not (res1 or res2):
            Utils.raise_with_error(
                "Failed to set both GH commit status and PR comment with Workflow Status, cannot proceed"
            )
        if env.PR_NUMBER:
            # TODO: enable for branch, add commit number limiting
            GitCommit.update_s3_data()

    @classmethod
    def pre_run(cls, _workflow, _job):
        result = Result.from_fs(_job.name)
        S3.copy_result_from_s3(
            Result.file_name_static(_workflow.name),
        )
        workflow_result = Result.from_fs(_workflow.name)
        workflow_result.update_sub_result(result)
        S3.copy_result_to_s3(
            workflow_result,
            unlock=True,
        )

    @classmethod
    def run(cls, _workflow, _job):
        pass

    @classmethod
    def post_run(cls, _workflow, _job, info_errors):
        result = Result.from_fs(_job.name)
        env = _Environment.get()
        S3.copy_result_from_s3(
            Result.file_name_static(_workflow.name),
            lock=True,
        )
        workflow_result = Result.from_fs(_workflow.name)
        print(f"Workflow info [{workflow_result.info}], info_errors [{info_errors}]")

        env_info = env.REPORT_INFO
        if env_info:
            print(
                f"WARNING: some info lines are set in Environment - append to report [{env_info}]"
            )
            info_errors += env_info
        if info_errors:
            info_errors = [f"    |  {error}" for error in info_errors]
            info_str = f"{_job.name}:\n"
            info_str += "\n".join(info_errors)
            print("Update workflow results with new info")
            workflow_result.set_info(info_str)

        old_status = workflow_result.status

        S3.upload_result_files_to_s3(result)
        workflow_result.update_sub_result(result)

        skipped_job_results = []
        if not result.is_ok():
            print(
                "Current job failed - find dependee jobs in the workflow and set their statuses to skipped"
            )
            workflow_config_parsed = WorkflowConfigParser(_workflow).parse()
            for dependee_job in workflow_config_parsed.workflow_yaml_config.jobs:
                if _job.name in dependee_job.needs:
                    if _workflow.get_job(dependee_job.name).run_unless_cancelled:
                        continue
                    print(
                        f"NOTE: Set job [{dependee_job.name}] status to [{Result.Status.SKIPPED}] due to current failure"
                    )
                    skipped_job_results.append(
                        Result(
                            name=dependee_job.name,
                            status=Result.Status.SKIPPED,
                            info=ResultInfo.SKIPPED_DUE_TO_PREVIOUS_FAILURE
                            + f" [{_job.name}]",
                        )
                    )
        for skipped_job_result in skipped_job_results:
            workflow_result.update_sub_result(skipped_job_result)

        S3.copy_result_to_s3(
            workflow_result,
            unlock=True,
        )
        if workflow_result.status != old_status:
            print(
                f"Update GH commit status [{result.name}]: [{old_status} -> {workflow_result.status}], link [{workflow_result.html_link}]"
            )
            GH.post_commit_status(
                name=workflow_result.name,
                status=GH.convert_to_gh_status(workflow_result.status),
                description="",
                url=workflow_result.html_link,
            )
