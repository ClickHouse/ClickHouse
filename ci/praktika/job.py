import copy
import fnmatch
import json
import os
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional

from . import Artifact
from .utils import Shell, Utils


class Job:
    @dataclass
    class Requirements:
        python: bool = False
        python_requirements_txt: str = ""

    @dataclass
    class CacheDigestConfig:
        include_paths: List[str] = field(default_factory=list)
        exclude_paths: List[str] = field(default_factory=list)
        # set to true if any submodule affects the job
        with_git_submodules: bool = False

    @dataclass
    class ParamSet:
        parameter: Optional[Any] = None
        runs_on: Optional[List[str]] = None
        provides: Optional[List[str]] = None
        requires: Optional[List[str]] = None
        timeout: Optional[int] = None

    @dataclass
    class Config:
        # Job Name
        name: str

        # Machine's label to run job on. For instance [ubuntu-latest] for free gh runner
        runs_on: List[str]

        # Job Run Command
        command: str

        # What job requires
        #   May be `Artifact.Config.name` (for physical artifacts) or `Job.Config.name` (for ordering only)
        requires: List[str] = field(default_factory=list)

        # If True, jobs listed in `requires` by `Job.Config.name` are treated as
        # hard dependencies: they must run (and cannot be skipped as unaffected)
        # unless their artifacts are already cached by CI.
        needs_jobs_from_requires: bool = False

        # What job provides
        #   May be only `Artifact.Config.name`
        provides: List[str] = field(default_factory=list)

        job_requirements: Optional["Job.Requirements"] = None

        timeout: int = 5 * 3600

        timeout_shell_cleanup: Optional[str] = None

        digest_config: Optional["Job.CacheDigestConfig"] = None

        run_in_docker: str = ""

        run_unless_cancelled: bool = False

        allow_merge_on_failure: bool = False

        enable_commit_status: bool = False

        enable_gh_auth: bool = False

        # If a job Result contains multiple sub-results, and only a specific sub-result should be sent to CIDB, set its name here.
        result_name_for_cidb: str = ""

        parameter: Any = None

        # List of commands to call upon job completion
        post_hooks: List[str] = field(default_factory=list)

        def parametrize(self, *param_sets: "Job.ParamSet"):
            res = []
            for param_set in param_sets:
                obj = copy.deepcopy(self)
                assert (
                    not obj.provides
                ), "Job.Config.provides must be empty for parametrized jobs"
                if param_set.parameter:
                    obj.parameter = param_set.parameter
                    obj.command = obj.command.format(PARAMETER=param_set.parameter)
                if param_set.runs_on:
                    obj.runs_on = param_set.runs_on
                if param_set.timeout:
                    obj.timeout = param_set.timeout
                if param_set.provides:
                    assert (
                        not obj.provides
                    ), "Job.Config.provides must be empty for parametrized jobs"
                    obj.provides = param_set.provides
                if param_set.requires:
                    assert (
                        not obj.requires
                    ), "Job.Config.requires and parametrize(requires=...) are both set"
                    obj.requires = param_set.requires
                obj.name = obj.get_job_name_with_parameter()
                res.append(obj)
            return res

        def get_job_name_with_parameter(self):
            name, parameter, runs_on = self.name, self.parameter, self.runs_on
            res = name
            name_params = []
            if parameter:
                if isinstance(parameter, list) or isinstance(parameter, dict):
                    name_params.append(json.dumps(parameter))
                else:
                    name_params.append(parameter)
            elif runs_on:
                assert isinstance(runs_on, list)
                name_params.append(json.dumps(runs_on))
            else:
                assert False
            if name_params:
                name_params = [str(param) for param in name_params]
                res += f" ({', '.join(name_params)})"

            self.name = res
            return res

        def __repr__(self):
            return self.name

        def copy(self):
            """
            To create an instant copy of a job config used in multiple workflows
            :return: Job.Config
            """
            return copy.deepcopy(self)

        def set_name(self, name):
            res = copy.deepcopy(self)
            res.name = name
            return res

        def set_dependency(self, job, reset=False):
            res = copy.deepcopy(self)
            if not (isinstance(job, list) or isinstance(job, tuple)):
                job = [job]
            if reset:
                res.requires = []
            for job_ in job:
                if isinstance(job_, str):
                    res.requires.append(job_)
                elif isinstance(job_, Job.Config):
                    res.requires.append(job_.name)
                else:
                    Utils.raise_with_error(f"Invalid dependency type [{job_}]")
            return res

        def set_provides(self, artifact_name, reset=False):
            res = copy.deepcopy(self)
            if not (
                isinstance(artifact_name, list) or isinstance(artifact_name, tuple)
            ):
                artifact_name = [artifact_name]
            if reset:
                res.provides = []
            for artifact_name_ in artifact_name:
                if isinstance(artifact_name_, str):
                    res.provides.append(artifact_name_)
                elif isinstance(artifact_name_, Artifact.Config):
                    res.provides.append(artifact_name_.name)
                else:
                    Utils.raise_with_error(
                        f"Invalid artifact type {type(artifact_name_)} for [{artifact_name_}]"
                    )
            return res

        def set_runs_on(self, runs_on):
            res = copy.deepcopy(self)
            res.runs_on = runs_on
            return res

        def set_command(self, command):
            res = copy.deepcopy(self)
            res.command = command
            return res

        def unset_provides(self, artifact_keyword):
            """
            removes artifact matching artifact_keyword
            :param artifact_keyword:
            :return: copied and modified Job.Config instance
            """
            res = copy.deepcopy(self)
            provides_res = []
            for artifact in res.provides:
                if artifact_keyword.lower() not in artifact.lower():
                    provides_res.append(artifact)
            res.provides = provides_res
            return res

        def set_allow_merge_on_failure(self, value=True):
            res = copy.deepcopy(self)
            res.allow_merge_on_failure = value
            return res

        def set_post_hooks(self, post_hooks):
            res = copy.deepcopy(self)
            res.post_hooks = post_hooks
            return res

        @staticmethod
        def get_job(job_configs, job_name):
            for job in job_configs:
                if job.name == job_name:
                    return job
            raise RuntimeError(f"Failed to find job [{job_name}] in [{job_configs}]")

        def get_docker_image_name(self):
            return self.run_in_docker.split("+")[0] if self.run_in_docker else ""

        def is_affected_by(self, changed_files: List[str]) -> bool:
            if changed_files is None:
                return True
            if len(changed_files) == 0:
                return False
            if not self.digest_config:
                return True
            normalized_files = [os.path.normpath(f) for f in changed_files]

            # Check all other include/exclude logic
            for file in normalized_files:
                # Check if excluded
                for exclude in self.digest_config.exclude_paths:
                    exclude_norm = os.path.normpath(exclude)
                    if file == exclude_norm or file.startswith(exclude_norm + os.sep):
                        break
                else:
                    # Check if included
                    for include in self.digest_config.include_paths:
                        include_norm = os.path.normpath(include)
                        if fnmatch.fnmatch(file, include_norm) or file.startswith(
                            include_norm + os.sep
                        ):
                            return True

            # Optionally check for submodule changes
            if self.digest_config.with_git_submodules:
                try:
                    submodule_paths_str = Shell.get_output(
                        command="git config --file .gitmodules --get-regexp path | awk '{print $2}'",
                        verbose=True,
                    )
                    if any(file in submodule_paths_str for file in normalized_files):
                        return True
                except Exception as e:
                    print(f"Warning: failed to check git submodules: {e}")

            return False

        def __post_init__(self):
            if self.timeout_shell_cleanup:
                return
            if self.run_in_docker:
                # the container name is always the same (praktika) for every image
                self.timeout_shell_cleanup = "docker rm -f praktika"
