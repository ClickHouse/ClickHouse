from dataclasses import dataclass, field
from typing import List, Optional

from praktika import Artifact, Job
from praktika.docker import Docker
from praktika.secret import Secret
from praktika.utils import Utils


class Workflow:
    class Event:
        PULL_REQUEST = "pull_request"
        PUSH = "push"

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

        def is_event_pull_request(self):
            return self.event == Workflow.Event.PULL_REQUEST

        def is_event_push(self):
            return self.event == Workflow.Event.PUSH

        def get_job(self, name):
            job = self.find_job(name)
            if not job:
                Utils.raise_with_error(
                    f"Failed to find job [{name}], workflow [{self.name}]"
                )
            return job

        def find_job(self, name, lazy=False):
            name = str(name)
            for job in self.jobs:
                if lazy:
                    if name.lower() in job.name.lower():
                        return job
                else:
                    if job.name == name:
                        return job
            return None

        def get_secret(self, name) -> Optional[Secret.Config]:
            name = str(name)
            names = []
            for secret in self.secrets:
                if secret.name == name:
                    return secret
                names.append(secret.name)
            print(f"ERROR: Failed to find secret [{name}], workflow secrets [{names}]")
            raise
