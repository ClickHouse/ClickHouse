import json
import os
from typing import Optional

from ci.praktika.docker import Docker
from ci.praktika.info import Info
from ci.praktika.utils import Utils

DOCKER_TAG = os.getenv("DOCKER_TAG", "latest")


class DockerImage:
    def __init__(self, name: str, version: Optional[str] = None):
        self.name = name
        if version is None:
            self.version = "latest"
        else:
            self.version = version

    def __str__(self):
        return f"{self.name}:{self.version}"

    def __repr__(self):
        return f"DockerImage({self.name}:{self.version})"

    def _warn_pull_retried(self, matched, attempt, attempts):
        # A job script holds no frame-local env, so Info() is the route here (as in
        # scripts/server_cleanup.py); _add_report_message dumps immediately.
        Info().add_workflow_warning(
            f"Job image pull failed with [{matched}] and was retried "
            f"({attempt}/{attempts}): {self}"
        )

    def pull_image(self, *, timeout_s=None, retries=None):
        # An omitted knob must fall through to Docker.pull_image's own default,
        # so it is left out of the call rather than passed as None.
        budget = {}
        if timeout_s is not None:
            budget["timeout_s"] = timeout_s
        if retries is not None:
            budget["retries"] = retries
        try:
            print(f"Pulling image {self} - start")
            Docker.pull_image(
                str(self), strict=True, on_retry=self._warn_pull_retried, **budget
            )
            print(f"Pulling image {self} - done")
        except Exception as ex:
            print(f"Got exception pulling docker: {ex}")
            raise ex
        return self

    @staticmethod
    def get_docker_image(image_name: str) -> "DockerImage":
        assert DOCKER_TAG and isinstance(
            DOCKER_TAG, str
        ), "DOCKER_TAG env must be provided"
        if "{" in DOCKER_TAG:
            tags_map = json.loads(DOCKER_TAG)
            assert (
                image_name in tags_map
            ), f"Image name [{image_name}] does not exist in provided DOCKER_TAG json string"
            arch_suffix = "_arm" if Utils.is_arm() else "_amd"
            return DockerImage(image_name, tags_map[image_name] + arch_suffix)
        # DOCKER_TAG is a tag itself
        return DockerImage(image_name, DOCKER_TAG)
