from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

if __name__ == "__main__":
    # Note, LSan does not compatible with debugger
    if "asan" not in Info().job_name:
        # With gdb we will capture stacktrace in case of abnormal termination and timeout (45 mins)
        command_launcher = f"timeout -s INT -v 45m gdb -batch -ex 'handle all nostop' -ex 'set print thread-events off' -ex run -ex bt -ex 'thread apply all bt' -arg"
    else:
        command_launcher = ""

    R = Result.from_gtest_run(
        unit_tests_path="./ci/tmp/unit_tests_dbms",
        command_launcher=command_launcher,
    )

    profraw_files = (
        Shell.get_output("find . -name '*.profraw'", verbose=True).strip().split("\n")
    )
    profraw_files = [f.strip() for f in profraw_files if f.strip()]

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

            # Merge all profraw files to current directory
            merged_file = "./unit-tests.profdata"
            merge_cmd = f"{llvm_profdata} merge -sparse -failure-mode=warn {' '.join(profraw_files)} -o {merged_file} 2>&1"
            merge_output = Shell.get_output(merge_cmd, verbose=True)

            # Check for corrupted files in the output
            corrupted_files = [
                line
                for line in merge_output.split("\n")
                if "invalid instrumentation profile" in line
                or "file header is corrupt" in line
            ]
            if corrupted_files:
                print(f"WARNING: Found {len(corrupted_files)} corrupted profraw files:")
                for corrupted in corrupted_files:
                    print(f"  {corrupted}")

    R.complete_job()
