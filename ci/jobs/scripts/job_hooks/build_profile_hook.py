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
    # The sccache-warmup build compiles master with the PR release build's
    # exact cmake flags (no official-build flag, debug symbols stripped, no
    # PGO/BOLT) and skips linking. Its object sizes and compile traces are the
    # only master data directly comparable to a PR build, so the "Build profile
    # diff" check baselines its object-size and per-TU compile-time sections on
    # it. The upload is reduced (see check) and small: only TUs recompiled by
    # the merged commits appear in the trace.
    "arm_release_pr_cache_warmup",
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

# On PRs only the aarch64 release build is profiled: it feeds the
# "Build profile diff" check (ci/jobs/build_profile_diff_job.py), which compares
# the PR's build profile against master. PR builds are far more frequent than
# master pushes, so the subset is kept minimal, and the time trace is uploaded
# reduced (see LogClusterBuildProfileQueries.insert_profile_data) to keep the
# per-build row count in check.
_PROFILED_BUILDS_PR = ("arm_release",)

# The linked release builds must produce every profile artifact: they always
# link (so the time trace holds at least the link events), always have final
# binaries to measure, and nm of the linked binary works under (Thin)LTO. A
# missing or empty artifact there is a broken producer - prepare-time-trace.sh
# regressed, the build layout changed, nm disappeared from the image - and it
# must fail the build job loudly instead of letting the "Build profile diff"
# check go green on "no data" (fail-close).
_REQUIRED_ARTIFACTS = {
    "amd_release": ("profile.json", "binary_sizes.txt", "binary_symbols.txt"),
    "arm_release": ("profile.json", "binary_sizes.txt", "binary_symbols.txt"),
    # The warmup build does not link: no binaries, no link trace, no symbols
    # (the per-object nm pass is skipped under LTO). On a master push where
    # every TU is an sccache hit even the time trace is legitimately empty.
    "arm_release_pr_cache_warmup": ("binary_sizes.txt",),
}


def _should_profile(build_type, is_pr=False):
    """Whether build profile telemetry is collected for this build variant."""
    if is_pr:
        return build_type in _PROFILED_BUILDS_PR
    return build_type in _PROFILED_BUILDS


def _has_data(file):
    """Whether prepare-time-trace.sh actually produced this artifact.

    Not every build emits every artifact: the per-object nm pass is skipped
    for LTO builds, cross-arch/non-Linux builds produce no readable objects at
    all (#84159), and a build that links nothing has no link trace.
    prepare-time-trace.sh uses `xargs -r`, so for those builds the artifact is
    left empty rather than holding a junk row. Whether an absent or empty file
    is acceptable for this build is decided by _REQUIRED_ARTIFACTS.
    """
    file = Path(file)
    return file.exists() and file.stat().st_size > 0


def _upload_profile_artifacts(build_type, start_time, artifacts):
    """Upload the profile artifacts this build produced.

    An artifact this build type is required to produce (_REQUIRED_ARTIFACTS)
    raises when missing or empty - fail-close, so a broken producer cannot
    turn into a false-green "no data" downstream. Optional artifacts are
    skipped. Upload failures are NOT swallowed either: an INSERT rejection
    propagates so the lost telemetry stays visible and the hook fails loudly.
    """
    required = _REQUIRED_ARTIFACTS.get(build_type, ())
    for insert, file in artifacts:
        if not _has_data(file):
            if Path(file).name in required:
                raise RuntimeError(
                    f"Build [{build_type}] produced no profile data in [{file}], "
                    "but this artifact is required for it (see _REQUIRED_ARTIFACTS): "
                    "the profile producer is broken"
                )
            print(f"No build profile data in [{file}], skipping upload")
            continue
        insert(build_name=build_type, start_time=start_time, file=file)


def check():
    is_pr = Info().pr_number > 0
    build_type = Info().job_name.split("(")[1].rstrip(")")
    assert build_type
    if not _should_profile(build_type, is_pr=is_pr):
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

        def insert_profile_data(build_name, start_time, file):
            # On PRs and for the warmup build the time trace is uploaded
            # reduced: only the event kinds the "Build profile diff" check
            # consumes. The bulk of a full trace is per-pass LLVM events from
            # the ThinLTO link, useless for the diff and too voluminous for
            # the shared cluster at PR rates.
            queries.insert_profile_data(
                build_name=build_name,
                start_time=start_time,
                file=file,
                reduced=is_pr or build_type == "arm_release_pr_cache_warmup",
            )

        _upload_profile_artifacts(
            build_type,
            check_start_time,
            [
                (insert_profile_data, profile_data_file),
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
    check()
