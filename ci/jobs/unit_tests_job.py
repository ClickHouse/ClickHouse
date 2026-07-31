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
        # With gdb we will capture stacktrace in case of abnormal termination and timeout (45 mins)
        command_launcher = "timeout -s INT -v 45m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex run -ex bt -ex 'thread apply all bt' -arg"
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

    # Name the profile after this job's own coverage artifact, so the aggregation
    # can tell which shards arrived from the filenames alone. JOB_CONFIG has been
    # through dump()/get() by the time a job body runs, so it is a plain dict
    # here and attribute access on it raises AttributeError.
    _job_config = Info().job_config
    assert (
        _job_config is not None
    ), "JOB_CONFIG is not set, cannot derive the coverage profile name"
    _provides = _job_config["provides"]
    assert (
        isinstance(_provides, list)
        and len(_provides) == 1
        and isinstance(_provides[0], str)
        and _provides[0]
    ), f"expected exactly one provided artifact name, got {_provides!r}"
    merged_file = f"./{_provides[0]}.profdata"

    # Drop any pre-existing profile at our target name before deciding whether to
    # merge at all. llvm-profdata truncates its -o target in place instead of
    # replacing it, so a failed merge would otherwise leave an older valid
    # profile for the uploader to publish as this shard's contribution. On CI the
    # pre-run `git clean` already removes it; this covers a bare local run, where
    # that clean is skipped.
    if os.path.exists(merged_file):
        print(f"Removing pre-existing {merged_file}")
        os.unlink(merged_file)

    if profraw_files:
        # A zero-length .profraw is silently ignored by llvm-profdata at every
        # --failure-mode, so it would drop one process's coverage with no signal
        # at all. Treat it as an incomplete shard and publish no profile.
        empty_files = [f for f in profraw_files if os.path.getsize(f) == 0]
        if empty_files:
            print(
                f"ERROR: {len(empty_files)} .profraw files are empty, so this shard's coverage "
                f"is incomplete; publishing no profile: {', '.join(empty_files)}"
            )
            profraw_files = []

    if profraw_files:
        # Merge profraw files into profdata
        print("Collecting and merging LLVM coverage files...")
        print(f"Found {len(profraw_files)} .profraw files")

        # Auto-detect available LLVM profdata tool
        llvm_profdata = None
        for ver in ["21", "20", "19", "18", "17", "16", ""]:
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
            # absent instead of contributing a silently short profile that drags
            # the total down. The clean path is byte-identical to =warn.
            merge_cmd = f"{llvm_profdata} merge -sparse -failure-mode=any {' '.join(profraw_files)} -o {merged_file} 2>&1"
            merge_output = Shell.get_output(merge_cmd, verbose=True)
            if not os.path.exists(merged_file):
                print(f"ERROR: coverage merge produced no profile:\n{merge_output}")

    R.complete_job()
