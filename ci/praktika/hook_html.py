import dataclasses
import json
from pathlib import Path
from typing import List

from ._environment import _Environment
from .info import Info
from .parser import WorkflowConfigParser
from .result import Result, ResultInfo, _ResultS3
from .runtime import RunConfig
from .s3 import S3
from .settings import Settings
from .usage import ComputeUsage, StorageUsage
from .utils import Utils


@dataclasses.dataclass
class GitCommit:
    sha: str
    message: str = ""
    # date: str

    @staticmethod
    def from_json(file) -> List["GitCommit"]:
        commits = []
        json_data = None
        try:
            with open(file, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            commits = [
                GitCommit(
                    message=commit["message"],
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
        commits.append(GitCommit(sha=sha, message=env.COMMIT_MESSAGE))
        commits = commits[
            -20:
        ]  # limit maximum number of commits from the past to show in the report
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
    def get_s3_path(cls):
        env = _Environment.get()
        if env.PR_NUMBER:
            s3suffix = f"PRs/{env.PR_NUMBER}"
        else:
            assert env.BRANCH
            s3suffix = f"REFs/{env.BRANCH}"
        return f"{Settings.HTML_S3_PATH}/{s3suffix}"

    @classmethod
    def pull_from_s3(cls):
        local_path = Path(cls.file_name())
        file_name = local_path.name
        s3_path = f"{cls.get_s3_path()}/{file_name}"
        if not S3.copy_file_from_s3(
            s3_path=s3_path, local_path=local_path, no_strict=True
        ):
            print(f"WARNING: failed to cp file [{s3_path}] from s3")
            return []
        return cls.from_json(local_path)

    @classmethod
    def push_to_s3(cls, commits):
        print(f"INFO: push commits data to s3, commits num [{len(commits)}]")
        cls.dump(commits)
        local_path = Path(cls.file_name())
        file_name = local_path.name
        s3_path = f"{cls.get_s3_path()}/{file_name}"
        if not S3.copy_file_to_s3(
            s3_path=s3_path, local_path=local_path, text=True, no_strict=True
        ):
            print(f"WARNING: failed to cp file [{local_path}] to s3")

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
    def push_pending_ci_report(cls, _workflow):
        # generate pending Results for all jobs in the workflow
        env = _Environment.get()
        results = []
        for job in _workflow.jobs:
            if job.name == Settings.CI_CONFIG_JOB_NAME:
                # fetch running status with start_time for current job
                result = Result.from_fs(job.name)
            else:
                result = Result.create_new(job.name, Result.Status.PENDING)
            results.append(result)
        summary_result = Result.create_new(
            _workflow.name, Result.Status.RUNNING, results=results
        )
        summary_result.start_time = Utils.timestamp()
        info = Info()
        report_url_current_sha = info.get_report_url(latest=False)
        summary_result.add_ext_key_value("pr_title", info.pr_title).add_ext_key_value(
            "git_branch", info.git_branch
        ).add_ext_key_value("report_url", report_url_current_sha).add_ext_key_value(
            "commit_sha", env.SHA
        ).add_ext_key_value(
            "commit_message", env.COMMIT_MESSAGE
        ).add_ext_key_value(
            "repo_name", env.REPOSITORY
        ).add_ext_key_value(
            "pr_number", env.PR_NUMBER
        ).add_ext_key_value(
            "run_url", env.RUN_URL
        ).add_ext_key_value(
            "change_url", env.CHANGE_URL
        ).add_ext_key_value(
            "workflow_name", env.WORKFLOW_NAME
        ).add_ext_key_value(
            "base_branch", env.BASE_BRANCH
        )

        summary_result.dump()
        # Use version 0 for initial workflow report creation (destructive reset)
        # This is safe here as it runs once at workflow start before any concurrent updates
        assert _ResultS3.copy_result_to_s3_with_version(summary_result, version=0)
        print(f"CI Status page url [{report_url_current_sha}]")

        GitCommit.update_s3_data()

    @classmethod
    def configure(cls, _workflow):
        # generate pending Results for all jobs in the workflow
        if _workflow.enable_cache:
            workflow_config = RunConfig.from_fs(_workflow.name)
            skipped_jobs = workflow_config.cache_success
            filtered_job_and_reason = workflow_config.filtered_jobs
            job_cache_records = workflow_config.cache_jobs
            results = []
            info = Info()
            for skipped_job in skipped_jobs:
                if skipped_job not in filtered_job_and_reason:
                    cache_record = job_cache_records[skipped_job]
                    report_link = info.get_specific_report_url(
                        pr_number=cache_record.pr_number,
                        branch=cache_record.branch,
                        sha=cache_record.sha,
                        job_name=skipped_job,
                        workflow_name=cache_record.workflow,
                    )
                    result = Result.create_new(
                        skipped_job,
                        Result.Status.SKIPPED,
                        [report_link],
                        "reused from cache",
                    )
                else:
                    result = Result.create_new(
                        skipped_job,
                        Result.Status.SKIPPED,
                        info=filtered_job_and_reason[skipped_job],
                    )
                results.append(result)
            if results:
                assert (
                    _ResultS3.update_workflow_results(
                        _workflow.name, new_sub_results=results
                    )
                    is None
                ), "Workflow status supposed to remain 'running'"

    @classmethod
    def pre_run(cls, _workflow, _job):
        result = Result.from_fs(_job.name)
        _ResultS3.update_workflow_results(
            workflow_name=_workflow.name, new_sub_results=result
        )

    @classmethod
    def run(cls, _workflow, _job):
        pass

    @classmethod
    def post_run(cls, _workflow, _job, info_errors):
        result = Result.from_fs(_job.name)
        env = _Environment.get()
        if env.WORKFLOW_JOB_DATA:
            result.add_ext_key_value(
                "run_url",
                f"{env.RUN_URL}/job/{env.WORKFLOW_JOB_DATA['check_run_id']}",
            )
        _ResultS3.upload_result_files_to_s3(result).dump()
        storage_usage = None
        if StorageUsage.exist():
            StorageUsage.add_uploaded(
                result.file_name()
            )  # add Result file beforehand to upload actual storage usage data
            print("Storage usage data found - add to Result")
            storage_usage = StorageUsage.from_fs()
            result.ext["storage_usage"] = storage_usage
        _ResultS3.copy_result_to_s3(result)

        new_sub_results = [result]
        new_result_info = ""
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
            new_result_info = info_str

        if not result.is_ok() and not result.do_not_block_pipeline_on_failure():
            print(
                "Current job failed - find dependee jobs in the workflow and set their statuses to dropped"
            )
            workflow_config_parsed = WorkflowConfigParser(_workflow).parse()

            dependees = set()

            def add_dependees(job_name):
                for dependee_job in workflow_config_parsed.workflow_yaml_config.jobs:
                    if dependee_job.run_unless_cancelled:
                        continue
                    if (
                        job_name in dependee_job.needs
                        and dependee_job.name not in dependees
                    ):
                        dependees.add(dependee_job.name)
                        add_dependees(dependee_job.name)

            add_dependees(_job.name)

            for dependee in dependees:
                print(
                    f"NOTE: Set job [{dependee}] status to [{Result.Status.DROPPED}] due to current failure"
                )
                new_sub_results.append(
                    Result(
                        name=dependee,
                        status=Result.Status.DROPPED,
                        info=ResultInfo.DROPPED_DUE_TO_PREVIOUS_FAILURE
                        + f" [{_job.name}]",
                        start_time=Utils.timestamp(),
                        duration=0,
                    )
                )

        updated_status = _ResultS3.update_workflow_results(
            new_info=new_result_info,
            new_sub_results=new_sub_results,
            workflow_name=_workflow.name,
            storage_usage=storage_usage,
            compute_usage=ComputeUsage().set_usage(
                runner_str="_".join(_job.runs_on),
                duration=result.duration,
                job_name=_job.name,
            ),
        )
        return updated_status
