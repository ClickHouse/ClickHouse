import copy
import json
from dataclasses import dataclass, field
from typing import Any, List, Optional

Jobs = List["Job.Config"]


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
            parameter: Optional[List[Optional[Any]]] = None,
            runs_on: Optional[List[Optional[List[str]]]] = None,
            provides: Optional[List[Optional[List[str]]]] = None,
            requires: Optional[List[Optional[List[str]]]] = None,
            timeout: Optional[List[Optional[int]]] = None,
        ) -> Jobs:
            assert (
                parameter or runs_on
            ), "Either :parameter or :runs_on must be non empty list for parametrisation"
            if runs_on:
                assert isinstance(runs_on, list) and isinstance(runs_on[0], list)
            if not parameter:
                # runs_on is asserted above
                parameter = [None] * len(runs_on)  # type: ignore[arg-type]
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
