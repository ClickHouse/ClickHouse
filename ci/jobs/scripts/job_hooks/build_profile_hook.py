import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.log_cluster import LogClusterBuildProfileQueries
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = "./ci/tmp"
build_dir = "./ci/tmp/build"


def check():
    print("Prepare build profile data")
    profiles_dir = Path("./ci/tmp") / "profiles_source"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    try:
        Shell.check(
            "./utils/prepare-time-trace/prepare-time-trace.sh "
            f"{build_dir} {profiles_dir.absolute()}",
            strict=True,
            verbose=True,
        )
        profile_data_file = Path(temp_dir) / "profile.json"
        with open(profile_data_file, "wb") as profile_fd:
            for profile_source in profiles_dir.iterdir():
                if profile_source.name not in (
                    "binary_sizes.txt",
                    "binary_symbols.txt",
                ):
                    with open(profile_source, "rb") as ps_fd:
                        profile_fd.write(ps_fd.read())
        check_start_time = Utils.timestamp_to_str(
            Result.from_fs(Info().job_name).start_time
        )
        build_type = Info().job_name.split("(")[1].rstrip(")")
        assert build_type
        LogClusterBuildProfileQueries().insert_profile_data(
            build_name=build_type,
            start_time=check_start_time,
            file=profile_data_file,
        )
        LogClusterBuildProfileQueries().insert_build_size_data(
            build_name=build_type,
            start_time=check_start_time,
            file=profiles_dir / "binary_sizes.txt",
        )
        LogClusterBuildProfileQueries().insert_binary_symbol_data(
            build_name=build_type,
            start_time=check_start_time,
            file=profiles_dir / "binary_symbols.txt",
        )
    except Exception as e:
        print(f"ERROR: Failed to upload build profile data:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    if Info().pr_number == 0:
        check()
    else:
        print("Not applicable for PRs")
