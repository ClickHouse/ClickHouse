import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.log_cluster import LogClusterBuildProfileQueries
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = "./ci/tmp"
build_dir = "./ci/tmp/build"


def _has_data(file):
    """Whether prepare-time-trace.sh actually produced this artifact.

    Not every build emits every artifact: binary_symbols.txt is skipped for
    LTO builds (nm does not work with LTO) and cross-arch/non-Linux builds
    produce no readable objects at all (#84159). A missing or empty file means
    there is nothing to upload, not that the job failed.
    """
    file = Path(file)
    return file.exists() and file.stat().st_size > 0


def _upload_profile_artifacts(build_type, start_time, artifacts):
    """Upload each (insert, file) pair that was produced; skip the rest."""
    for insert, file in artifacts:
        if not _has_data(file):
            print(f"No build profile data in [{file}], skipping upload")
            continue
        insert(build_name=build_type, start_time=start_time, file=file)


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
        build_size_file = profiles_dir / "binary_sizes.txt"
        binary_symbol_file = profiles_dir / "binary_symbols.txt"
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
        queries = LogClusterBuildProfileQueries()
        _upload_profile_artifacts(
            build_type,
            check_start_time,
            [
                (queries.insert_profile_data, profile_data_file),
                (queries.insert_build_size_data, build_size_file),
                (queries.insert_binary_symbol_data, binary_symbol_file),
            ],
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
