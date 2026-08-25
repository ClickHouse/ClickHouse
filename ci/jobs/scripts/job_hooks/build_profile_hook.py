import re
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
    # every TU is an sccache hit even the time trace is legitimately empty -
    # a broken trace extractor is told apart from that case by
    # _verify_trace_extraction instead.
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


# The raw -ftime-trace outputs clang leaves in the build directory - the same
# pattern prepare-time-trace.sh extracts from. One file appears per actually
# compiled TU (an sccache hit does not run the compiler) plus one per linked
# binary, so their presence is the ground truth for whether this build was
# expected to produce time-trace rows.
_RAW_TRACE_RE = re.compile(r"\.(c|cpp|cc|cxx)\.json$|\.time-trace$")


def _raw_traces_exist(directory):
    directory = Path(directory)
    if not directory.is_dir():
        return False
    for pattern in ("*.json", "*.time-trace"):
        for file in directory.rglob(pattern):
            if _RAW_TRACE_RE.search(file.name):
                return True
    return False


def _verify_trace_extraction(build_directory, profile_data_file):
    """Fail when the compiler emitted raw traces but extraction produced nothing.

    The warmup build's time trace cannot be unconditionally required
    (_REQUIRED_ARTIFACTS): when every TU is an sccache hit it is legitimately
    empty. That makes a regression in the trace extractor indistinguishable
    from the all-cache-hit case downstream - binary_sizes.txt alone keeps
    find_warmup_baseline succeeding while the "Build profile diff" per-TU
    compile-time section quietly loses its baseline rows. Disambiguate with
    the ground truth: raw trace files exist iff the compiler actually ran, so
    raw traces present + nothing extracted = a broken producer, and the hook
    must fail loudly instead of uploading a silently incomplete profile.
    """
    if _has_data(profile_data_file):
        return
    if _raw_traces_exist(build_directory):
        raise RuntimeError(
            f"The build left raw -ftime-trace files in [{build_directory}] but "
            f"nothing was extracted into [{profile_data_file}]: the trace "
            "extraction (prepare-time-trace.sh) is broken"
        )


# The final linked binaries the "Build profile diff" check compares per binary
# (FINAL_BINARIES in ci/jobs/build_profile_diff_job.py), relative to the build
# directory.
_FINAL_BINARIES = ("programs/clickhouse", "programs/clickhouse-keeper")
# The headline size comparison (HEADLINE_BINARIES in build_profile_diff_job.py)
# reads the stripped binary. It carries no symbols by construction, so only its
# size row is required.
_SIZE_ONLY_BINARIES = ("programs/clickhouse-stripped",)


def _any_line(artifact_file, matches):
    with open(artifact_file) as fd:
        for line in fd:
            if matches(line):
                return True
    return False


def _verify_final_binary_coverage(build_directory, build_size_file, binary_symbol_file):
    """Fail when a linked final binary is missing from the produced artifacts.

    _REQUIRED_ARTIFACTS guards whole files only: binary_symbols.txt can be
    non-empty while nm silently failed on one of the final binaries, and the
    "Build profile diff" sections would then silently omit that binary - a
    `clickhouse-keeper`-only regression turned false-green. The filesystem is
    the ground truth: every final binary present as a real file (not the
    symlink of a non-standalone keeper, which the producer legitimately
    skips) must have its row in binary_sizes.txt (`wc -c` lines ending with
    the path) and its rows in binary_symbols.txt (nm lines starting with the
    path). The stripped binary - the headline size comparison's only input -
    must have its size row too (it has no symbols by construction). Whole-file
    absence is left to _REQUIRED_ARTIFACTS to report.
    """
    for rel in _FINAL_BINARIES + _SIZE_ONLY_BINARIES:
        binary = Path(build_directory) / rel
        if binary.is_symlink() or not binary.is_file():
            continue
        path = f"{build_directory}/{rel}"
        checks = [
            (build_size_file, lambda line: line.split() and line.split()[-1] == path, "size"),
        ]
        if rel in _FINAL_BINARIES:
            checks.append((binary_symbol_file, lambda line: line.startswith(path + " "), "symbol"))
        for artifact, matches, what in checks:
            if _has_data(artifact) and not _any_line(artifact, matches):
                raise RuntimeError(
                    f"The build linked [{path}] but [{artifact}] holds no {what} "
                    "rows for it: the profile producer lost this binary, and the "
                    '"Build profile diff" check would silently omit it'
                )


# The order the profile artifacts are uploaded in. binary_sizes.txt goes LAST
# on purpose: `find_baseline` / `find_warmup_baseline` in
# ci/jobs/build_profile_diff_job.py pick the master baseline by looking for a
# `binary_sizes` row, so that row doubles as the marker saying "this commit's
# profile is complete". Written last, it can only exist once the trace and the
# symbols are already in - a master build whose `binary_symbols` INSERT is
# rejected, or that loses the connection halfway, simply never becomes a
# baseline candidate and the previous complete commit stays the baseline.
# Written first, that half-uploaded build would become the canonical baseline
# and silently strip the symbol section off every PR compared against it
# (fail-close).
_UPLOAD_ORDER = ("profile.json", "binary_symbols.txt", "binary_sizes.txt")


def _in_upload_order(artifacts):
    """Order the artifacts so that the completion marker is uploaded last.

    An artifact missing from _UPLOAD_ORDER raises rather than being appended
    somewhere: a new artifact whose place in the ordering was not considered
    could be the one that ends up after the marker.
    """
    unknown = [str(file) for _, file in artifacts if Path(file).name not in _UPLOAD_ORDER]
    if unknown:
        raise RuntimeError(
            f"Profile artifacts {unknown} have no place in the upload order "
            "(see _UPLOAD_ORDER): the completion marker must stay last"
        )
    return sorted(artifacts, key=lambda pair: _UPLOAD_ORDER.index(Path(pair[1]).name))


def _upload_profile_artifacts(build_type, start_time, artifacts):
    """Upload the profile artifacts this build produced.

    An artifact this build type is required to produce (_REQUIRED_ARTIFACTS)
    raises when missing or empty - fail-close, so a broken producer cannot
    turn into a false-green "no data" downstream. Optional artifacts are
    skipped. Upload failures are NOT swallowed either: an INSERT rejection
    propagates so the lost telemetry stays visible and the hook fails loudly.
    Both of those stop the upload where they happen, which is why the order is
    not the caller's to choose (see _UPLOAD_ORDER).
    """
    required = _REQUIRED_ARTIFACTS.get(build_type, ())
    for insert, file in _in_upload_order(artifacts):
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
        _verify_trace_extraction(build_dir, profile_data_file)
        _verify_final_binary_coverage(build_dir, build_size_file, binary_symbol_file)
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
            # Listed in _UPLOAD_ORDER, which is also what the upload follows:
            # binary_sizes.txt last, as the completion marker.
            [
                (insert_profile_data, profile_data_file),
                (queries.insert_binary_symbol_data, binary_symbol_file),
                (queries.insert_build_size_data, build_size_file),
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
