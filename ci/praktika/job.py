import copy
import json
from dataclasses import dataclass, field
from typing import Any, List, Optional

from . import Artifact
from .utils import Utils


class Job:
    @dataclass
    class Requirements:
        python: bool = False
        python_requirements_txt: str = ""

    @dataclass
    class CacheDigestConfig:
        include_paths: List[str] = field(default_factory=list)
        exclude_paths: List[str] = field(default_factory=list)
        with_git_submodules: bool = False

    @dataclass
    class Config:
        # Job Name
        name: str

        # Machine's label to run job on. For instance [ubuntu-latest] for free gh runner
        runs_on: List[str]

        # Job Run Command
        command: str

        # What job requires
        #   May be phony or physical names
        requires: List[str] = field(default_factory=list)

        # What job provides
        #   May be phony or physical names
        provides: List[str] = field(default_factory=list)

        job_requirements: Optional["Job.Requirements"] = None

        timeout: int = 5 * 3600

        digest_config: Optional["Job.CacheDigestConfig"] = None

        run_in_docker: str = ""

        run_unless_cancelled: bool = False

        allow_merge_on_failure: bool = False

        enable_commit_status: bool = False

        # If a job Result contains multiple sub-results, and only a specific sub-result should be sent to CIDB, set its name here.
        result_name_for_cidb: str = ""

        parameter: Any = None

        # List of commands to call upon job completion
        post_hooks: List[str] = field(default_factory=list)

        def parametrize(
            self,
            parameter: Optional[List[Any]] = None,
            runs_on: Optional[List[List[str]]] = None,
            provides: Optional[List[List[str]]] = None,
            requires: Optional[List[List[str]]] = None,
            timeout: Optional[List[int]] = None,
        ):
            assert (
                parameter or runs_on
            ), "Either :parameter or :runs_on must be non empty list for parametrisation"
            if runs_on:
                assert isinstance(runs_on, list) and isinstance(runs_on[0], list)
            if not parameter:
                parameter = [None] * len(runs_on)
            if not runs_on:
                runs_on = [None] * len(parameter)
            if not timeout:
                timeout = [None] * len(parameter)
            if not provides:
                provides = [None] * len(parameter)
            if not requires:
                requires = [None] * len(parameter)
            assert (
                len(parameter)
                == len(runs_on)
                == len(timeout)
                == len(provides)
                == len(requires)
            ), f"Parametrization lists must be of the same size [{len(parameter)}, {len(runs_on)}, {len(timeout)}, {len(provides)}, {len(requires)}]"

            res = []
            for parameter_, runs_on_, timeout_, provides_, requires_ in zip(
                parameter, runs_on, timeout, provides, requires
            ):
                obj = copy.deepcopy(self)
                assert (
                    not obj.provides
                ), "Job.Config.provides must be empty for parametrized jobs"
                if parameter_:
                    obj.parameter = parameter_
                    obj.command = obj.command.format(PARAMETER=parameter_)
                if runs_on_:
                    obj.runs_on = runs_on_
                if timeout_:
                    obj.timeout = timeout_
                if provides_:
                    assert (
                        not obj.provides
                    ), "Job.Config.provides must be empty for parametrized jobs"
                    obj.provides = provides_
                if requires_:
                    assert (
                        not obj.requires
                    ), "Job.Config.requires and parametrize(requires=...) are both set"
                    obj.requires = requires_
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

        @staticmethod
        def get_job(job_configs, job_name):
            for job in job_configs:
                if job.name == job_name:
                    return job
            raise RuntimeError(f"Failed to find job [{job_name}] in [{job_configs}]")
