import os

from praktika.utils import MetaClasses


class ScalingType(metaclass=MetaClasses.WithIter):
    DISABLED = "disabled"
    AUTOMATIC_SCALE_DOWN = "scale_down"
    AUTOMATIC_SCALE_UP_DOWN = "scale"


class DefaultExecutionSettings:
    GH_ACTIONS_DIRECTORY: str = "/home/ubuntu/gh_actions"
    RUNNER_SCALING_TYPE: str = ScalingType.AUTOMATIC_SCALE_UP_DOWN
    MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC: int = 30


class ExecutionSettings:
    GH_ACTIONS_DIRECTORY = os.getenv(
        "GH_ACTIONS_DIRECTORY", DefaultExecutionSettings.GH_ACTIONS_DIRECTORY
    )
    RUNNER_SCALING_TYPE = os.getenv(
        "RUNNER_SCALING_TYPE", DefaultExecutionSettings.RUNNER_SCALING_TYPE
    )
    MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC = int(
        os.getenv(
            "MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC",
            DefaultExecutionSettings.MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC,
        )
    )
    LOCAL_EXECUTION = bool(os.getenv("CLOUD", "0") == "0")
