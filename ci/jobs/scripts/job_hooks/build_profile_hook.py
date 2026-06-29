import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.log_cluster import LogClusterBuildProfileQueries
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = "./ci/tmp"
build_dir = "./ci/tmp/build"

# Build profile telemetry is collected only for this explicit subset of builds.
# The analytics DB is shared by every Build variant; on a master push ~25
# variants finish and upload near-simultaneously, and the combined load crosses
# the cluster's per-user memory limit (Code 241 MEMORY_LIMIT_EXCEEDED). Two
# representative release builds keep the profile we actually look at (the
# production binaries) while removing that contention at the source. Uncomment a
# build below to profile it too; the full set of build names is BuildTypes in
# ci/defs/defs.py.
_PROFILED_BUILDS = (
    "amd_release",
    "arm_release",
    # "amd_debug",
    # "arm_debug",
    # "amd_binary",
    # "arm_binary",
    # "amd_asan_ubsan",
    # "arm_asan_ubsan",
    # "amd_tsan",
    # "arm_tsan",
    # "amd_msan",
    # "arm_msan",
    # "arm_ubsan",
    # "amd_darwin",
    # "arm_darwin",
)


def _should_profile(build_type):
    """Whether build profile telemetry is collected for this build variant."""
    return build_type in _PROFILED_BUILDS


def _has_data(file):
    """Whether prepare-time-trace.sh actually produced this artifact.

    Not every build emits every artifact: binary_symbols.txt is skipped for
    LTO builds (nm does not work with LTO) and cross-arch/non-Linux builds
    produce no readable objects at all (#84159). prepare-time-trace.sh uses
    `xargs -r`, so for those builds the artifact is left empty rather than
    holding a junk row. An absent or empty file therefore means "this build
    has no such profile data to upload", not that the job failed.
    """
    file = Path(file)
    return file.exists() and file.stat().st_size > 0


def _upload_profile_artifacts(build_type, start_time, artifacts):
    """Upload the profile artifacts this build produced.

    Skips artifacts that are genuinely empty (no data for this build, see
    _has_data). Upload failures are NOT swallowed: an INSERT rejection
    propagates so the lost telemetry stays visible and the hook fails loudly.
    """
    for insert, file in artifacts:
        if not _has_data(file):
            print(f"No build profile data in [{file}], skipping upload")
            continue
        insert(build_name=build_type, start_time=start_time, file=file)


def check():
    build_type = Info().job_name.split("(")[1].rstrip(")")
    assert build_type
    if not _should_profile(build_type):
        print(f"Build profile telemetry not collected for [{build_type}]")
        return
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
    except Exception:
        # Fail loudly on any error producing, assembling, or uploading the
        # profile data: never let the post-hook pass silently.
        print("ERROR: Failed to collect build profile data:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    if Info().pr_number == 0:
        check()
    else:
        print("Not applicable for PRs")
