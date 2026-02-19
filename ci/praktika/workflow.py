from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from . import Artifact, Job
from .docker import Docker
from .secret import Secret
from .utils import Utils


class Workflow:
    class Event:
        PULL_REQUEST = "pull_request"
        PUSH = "push"
        SCHEDULE = "schedule"
        DISPATCH = "dispatch"
        MERGE_QUEUE = "merge_queue"

    @dataclass
    class Config:
        """
        branches - List of branch names or patterns, for push trigger only
        base_branches - List of base branches (target branch), for pull_request trigger only
        """

        name: str
        event: str
        jobs: List[Job.Config]
        branches: List[str] = field(default_factory=list)
        base_branches: List[str] = field(default_factory=list)
        artifacts: List[Artifact.Config] = field(default_factory=list)
        dockers: List[Docker.Config] = field(default_factory=list)
        secrets: List[Secret.Config] = field(default_factory=list)
        enable_job_filtering_by_changes: bool = False
        enable_cache: bool = False
        enable_report: bool = False
        # do a best effort to merge the PR if all jobs are successful
        enable_automerge: bool = False
        enable_merge_ready_status: bool = False
        enable_gh_summary_comment: bool = False
        enable_commit_status_on_failure: bool = False
        enable_cidb: bool = False
        enable_merge_commit: bool = False
        cron_schedules: List[str] = field(default_factory=list)
        inputs: List["Workflow.Config.InputConfig"] = field(default_factory=list)
        pre_hooks: List[Union[str, callable]] = field(default_factory=list)
        workflow_filter_hooks: List[callable] = field(default_factory=list)
        post_hooks: List[str] = field(default_factory=list)
        # If the Docker images specified in .dockers are intended to be built in a different workflow,
        #   their build process in this workflow can be disabled by setting this to True.
        disable_dockers_build: bool = False
        # If Docker images built for multiple platforms and merging them into a single manifest is required in this workflow
        enable_dockers_manifest_merge: bool = False
        # If latest tag shpuld be added for merged docker manifest, enable with .enable_dockers_manifest_merge
        set_latest_for_docker_merged_manifest: bool = False
        # if enabled, Finish job will fetch flaky check catalog and match failed tests with them
        enable_flaky_tests_catalog: bool = False
        # Job aliases for easy job reference with `praktika run job_alias --test TEST_NAME` in local environment
        job_aliases: Dict[str, str] = field(default_factory=dict)

        def is_event_pull_request(self):
            return self.event == Workflow.Event.PULL_REQUEST

        def is_event_push(self):
            return self.event == Workflow.Event.PUSH

        def is_event_schedule(self):
            return self.event == Workflow.Event.SCHEDULE

        def is_event_dispatch(self):
            return self.event == Workflow.Event.DISPATCH

        def get_job(self, name):
            jobs = self.find_jobs(name)
            if not jobs:
                Utils.raise_with_error(
                    f"Failed to find job [{name}], workflow [{self.name}]"
                )
                assert len(jobs) == 1
            return jobs[0]

        def find_jobs(self, name, lazy=False):
            if self.job_aliases and name in self.job_aliases:
                print(
                    f"NOTE: job alias [{name}] refers to job [{self.job_aliases[name]}]"
                )
                name = self.job_aliases[name]
            name = str(name)
            res = []
            for job in self.jobs:
                if lazy:
                    tokens = name.lower().split("*")
                    match = True
                    for token in tokens:
                        if token in job.name.lower():
                            continue
                        else:
                            match = False
                            break
                    if match:
                        res.append(job)
                    if name.lower() == job.name.lower():
                        # exact match - consider it as requested job
                        return [job]
                else:
                    if job.name == name:
                        res.append(job)
            return res

        def get_secret(self, name) -> Optional[Secret.Config]:
            name = str(name)
            names = []
            for secret in self.secrets:
                if secret.name == name:
                    return secret
                names.append(secret.name)
            print(f"ERROR: Failed to find secret [{name}], workflow secrets [{names}]")
            raise

        def _enabled_workflow_config(self):
            return (
                self.enable_cache
                or self.enable_report
                or self.dockers
                or self.enable_merge_ready_status
                or self.pre_hooks
            )

        @dataclass
        class InputConfig:
            name: str
            description: str
            is_required: bool
            default_value: str
            options: Optional[List] = None
