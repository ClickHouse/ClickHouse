import argparse

from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    BUILD = "build"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("BUILD_TYPE", help="Type: <amd|arm_debug|release_sanitizer>")
    parser.add_argument("--param", help="Optional custom job start stage", default=None)
    return parser.parse_args()


def main():

    args = parse_args()

    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    res = True
    results = []

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        info = Shell.get_output(f"ls -l {Settings.INPUT_DIR}")
        results.append(Result(name="TEST", status=Result.Status.SUCCESS, info=info))
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
