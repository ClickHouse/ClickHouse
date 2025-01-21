import copy
import json
from dataclasses import dataclass, field
from typing import Any, List, Optional


class Job:
    @dataclass
    class Requirements:
        python: bool = False
        python_requirements_txt: str = ""

    @dataclass
    class CacheDigestConfig:
        include_paths: List[str] = field(default_factory=list)
        exclude_paths: List[str] = field(default_factory=list)

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

        timeout: int = 1 * 3600

        digest_config: Optional["Job.CacheDigestConfig"] = None

        run_in_docker: str = ""

        run_unless_cancelled: bool = False

        allow_merge_on_failure: bool = False

        parameter: Any = None

        def parametrize(
            self,
            parameter: Optional[List[Any]] = None,
            runs_on: Optional[List[List[str]]] = None,
            timeout: Optional[List[int]] = None,
        ):
            assert (
                parameter or runs_on
            ), "Either :parameter or :runs_on must be non empty list for parametrisation"
            if not parameter:
                parameter = [None] * len(runs_on)
            if not runs_on:
                runs_on = [None] * len(parameter)
            if not timeout:
                timeout = [None] * len(parameter)
            assert (
                len(parameter) == len(runs_on) == len(timeout)
            ), "Parametrization lists must be of the same size"

            res = []
            for parameter_, runs_on_, timeout_ in zip(parameter, runs_on, timeout):
                obj = copy.deepcopy(self)
                if parameter_:
                    obj.parameter = parameter_
                if runs_on_:
                    obj.runs_on = runs_on_
                if timeout_:
                    obj.timeout = timeout_
                obj.name = obj.get_job_name_with_parameter()
                res.append(obj)
            return res

        def get_job_name_with_parameter(self):
            name, parameter, runs_on = self.name, self.parameter, self.runs_on
            res = name
            name_params = []
            if isinstance(parameter, list) or isinstance(parameter, dict):
                name_params.append(json.dumps(parameter))
            elif parameter is not None:
                name_params.append(parameter)
            if runs_on:
                assert isinstance(runs_on, list)
                name_params.append(json.dumps(runs_on))
            if name_params:
                name_params = [str(param) for param in name_params]
                res += f" ({', '.join(name_params)})"

            self.name = res
            return res

        def __repr__(self):
            return self.name
