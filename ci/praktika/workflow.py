from dataclasses import dataclass, field
from typing import Any, List, Optional

from . import Artifact, Job
from .docker import Docker
from .secret import Secret
from .utils import Utils


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
        enable_merge_commit: bool = False

        def is_event_pull_request(self) -> bool:
            return self.event == Workflow.Event.PULL_REQUEST

        def is_event_push(self) -> bool:
            return self.event == Workflow.Event.PUSH

        def get_job(self, name: str) -> Job.Config:
            job = self.find_job(name)
            if job is None:
                Utils.raise_with_error(
                    f"Failed to find job [{name}], workflow [{self.name}]"
                )
            return job

        def find_job(self, name: str, lazy: bool = False) -> Optional[Job.Config]:
            name = str(name)
            for job in self.jobs:
                if lazy:
                    if name.lower() in job.name.lower():
                        return job
                else:
                    if job.name == name:
                        return job
            return None

        def get_secret(self, name: Any) -> Secret.Config:
            name = str(name)
            names = []
            for secret in self.secrets:
                if secret.name == name:
                    return secret
                names.append(secret.name)
            Utils.raise_with_error(
                f"ERROR: Failed to find secret [{name}], workflow secrets [{names}]"
            )


Workflows = List[Workflow.Config]
