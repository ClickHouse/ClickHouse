from dataclasses import dataclass, field
from typing import List, Optional

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
        enable_cache: bool = False
        enable_report: bool = False
        enable_merge_ready_status: bool = False
        enable_cidb: bool = False
        enable_merge_commit: bool = False
        cron_schedules: List[str] = field(default_factory=list)

        def is_event_pull_request(self):
            return self.event == Workflow.Event.PULL_REQUEST

        def is_event_push(self):
            return self.event == Workflow.Event.PUSH

        def is_event_schedule(self):
            return self.event == Workflow.Event.SCHEDULE

        def get_job(self, name):
            jobs = self.find_jobs(name)
            if not jobs:
                Utils.raise_with_error(
                    f"Failed to find job [{name}], workflow [{self.name}]"
                )
                assert len(jobs) == 1
            return jobs[0]

        def find_jobs(self, name, lazy=False):
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
