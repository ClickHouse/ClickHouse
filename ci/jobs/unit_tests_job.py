import argparse
import os

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gtest_filter", default="")
    args = parser.parse_args()

    # Our static OpenSSL must ignore the image's system openssl.cnf.
    os.environ["OPENSSL_CONF"] = "/dev/null"

    job_name = Info().job_name

    # Note, LSan does not compatible with debugger
    if "asan" not in job_name:
        # With gdb we will capture stacktrace in case of abnormal termination and timeout.
        # The tests run sequentially in a single binary, so the timeout must cover the sum of
        # all test durations: under TSan that sum has grown to 38-40 minutes on average, and a
        # run on a slower machine regularly exceeded the previous 45-minute budget, blaming
        # whichever test happened to be running at that moment. Keep several tens of minutes
        # of headroom (the praktika job-level timeout is 5 hours).
        command_launcher = "timeout -s INT -v 90m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex run -ex bt -ex 'thread apply all bt' -arg"
    else:
        command_launcher = ""

    R = Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
        command_launcher=command_launcher,
        gtest_filter=args.gtest_filter,
    )

    profraw_files = (
        Shell.get_output("find . -name '*.profraw'", verbose=True).strip().split("\n")
    )
    profraw_files = [f.strip() for f in profraw_files if f.strip()]

    # All coverage bookkeeping stays inside this guard: only the instrumented
    # job emits .profraw files and declares a coverage artifact; the sanitizer
    # unit-test jobs emit none and must not touch JOB_CONFIG here.
    if profraw_files:
        # Merge profraw files into profdata
        print("Collecting and merging LLVM coverage files...")
        print(f"Found {len(profraw_files)} .profraw files")

        # Name the profile after this job's own coverage artifact, so the
        # aggregation can tell which shards arrived from the filenames alone.
        # JOB_CONFIG has been through dump()/get() by the time a job body runs,
        # so it is a plain dict here.
        provides = (Info().job_config or {}).get("provides")
        assert (
            isinstance(provides, list)
            and len(provides) == 1
            and isinstance(provides[0], str)
            and provides[0]
        ), f"expected exactly one provided artifact name, got {provides!r}"
        merged_file = f"./{provides[0]}.profdata"

        # llvm-profdata truncates its -o target in place instead of replacing
        # it, so a stale profile at the target name must be removed first.
        if os.path.exists(merged_file):
            print(f"Removing pre-existing {merged_file}")
            os.unlink(merged_file)

        # ERROR means the binary died before writing gtest.json, so the .profraw
        # covers only part of the run; FAIL is a completed run and still publishes.
        if R.is_error():
            print(
                "ERROR: the unit-test binary did not run to completion, so this "
                "shard's coverage is incomplete; publishing no profile"
            )
            profraw_files = []

        # A zero-length .profraw is silently accepted by llvm-profdata at every
        # --failure-mode; treat it as an incomplete shard and publish no profile.
        empty_files = [f for f in profraw_files if os.path.getsize(f) == 0]
        if empty_files:
            print(
                f"ERROR: {len(empty_files)} .profraw files are empty, so this shard's "
                f"coverage is incomplete; publishing no profile: {', '.join(empty_files)}"
            )
            profraw_files = []

    if profraw_files:
        # Auto-detect available LLVM profdata tool
        llvm_profdata = None
        for ver in ["22", "21", "20", "19", "18", "17", "16", ""]:
            cmd = f"llvm-profdata{'-' + ver if ver else ''}"
            if Shell.check(f"command -v {cmd}", verbose=False):
                llvm_profdata = cmd
                break

        if not llvm_profdata:
            print("ERROR: llvm-profdata not found in PATH")
        else:
            print(f"Using {llvm_profdata} to merge coverage files")

            # --failure-mode=any makes the merge all-or-nothing: on any invalid
            # input it exits non-zero and writes no file, so the shard is simply
            # absent (and the aggregate job reports SKIPPED with the shard name)
            # instead of contributing a silently short profile.
            merge_cmd = f"{llvm_profdata} merge -sparse -failure-mode=any {' '.join(profraw_files)} -o {merged_file} 2>&1"
            merge_output = Shell.get_output(merge_cmd, verbose=True)
            if not os.path.exists(merged_file):
                print(f"ERROR: coverage merge produced no profile:\n{merge_output}")

    # Failing unit tests are still a complete coverage measurement, and the runner
    # uploads `provides` artifacts on failure only when this flag is set.
    R.complete_job(do_not_block_pipeline_on_failure="llvm_coverage" in job_name)
