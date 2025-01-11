import configparser
import os
import subprocess
from pathlib import Path

from ci.praktika.result import Result
from ci.workflows.pull_request import S3_BUILDS_BUCKET
from praktika.utils import Utils, Shell
from praktika.s3 import S3

DEBUGGER = os.getenv("DEBUGGER", "")
TIMEOUT = int(os.getenv("TIMEOUT", "30"))
temp_dir = f"{Utils.cwd()}/ci/tmp/"
output_dir = f"{temp_dir}/fuzz_output"


def download_corpus():
    print("Downloading corpus...")
    s3_corpus_path = f"{S3_BUILDS_BUCKET}/fuzzer/corpus.zip"
    fuzzers_path = f"{temp_dir}/fuzzers"
    Shell.check(f"mkdir -p {fuzzers_path}")
    res = S3.copy_file_from_s3(s3_path=s3_corpus_path, local_path=fuzzers_path) and \
        Shell.check(f"cd {fuzzers_path} && unzip corpus.zip && rm {fuzzers_path}/corpus.zip", verbose=True)
    units = 0
    if res:
        for _, _, files in os.walk(fuzzers_path):
            units += len(files)
    print(f"...downloaded {units} units")
    return res


def run_fuzzer(fuzzer: str, timeout: int):
    timeout_hard = timeout + 60
    print(f"Running fuzzer {fuzzer} for {timeout} seconds (hard timeout is {timeout_hard})...")

    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_dir = f"corpus/{fuzzer}"
    if not os.path.exists(active_corpus_dir):
        os.makedirs(active_corpus_dir)
    options_file = f"{fuzzer}.options"
    custom_libfuzzer_options = ""
    fuzzer_arguments = ""
    use_fuzzer_args = False

    with Path(options_file) as path:
        if path.exists() and path.is_file():
            parser = configparser.ConfigParser()
            parser.read(path)

            if parser.has_section("asan"):
                os.environ["ASAN_OPTIONS"] = (
                    f"{os.environ['ASAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['asan'].items())}"
                )

            if parser.has_section("msan"):
                os.environ["MSAN_OPTIONS"] = (
                    f"{os.environ['MSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['msan'].items())}"
                )

            if parser.has_section("ubsan"):
                os.environ["UBSAN_OPTIONS"] = (
                    f"{os.environ['UBSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['ubsan'].items())}"
                )

            if parser.has_section("libfuzzer"):
                custom_libfuzzer_options = " ".join(
                    f"-{key}={value}"
                    for key, value in parser["libfuzzer"].items()
                    if key not in ("jobs", "exact_artifact_path")
                )

            if parser.has_section("fuzzer_arguments"):
                fuzzer_arguments = " ".join(
                    (f"{key}") if value == "" else (f"{key}={value}")
                    for key, value in parser["fuzzer_arguments"].items()
                )

            use_fuzzer_args = parser.getboolean("CI", "FUZZER_ARGS", fallback=False)

    exact_artifact_path = f"{output_dir}/{fuzzer}.unit"
    status_path = f"{output_dir}/{fuzzer}.status"
    out_path = f"{output_dir}/{fuzzer}.out"
    stdout_path = f"{output_dir}/{fuzzer}.stdout"

    if not "-dict=" in custom_libfuzzer_options and Path(f"{fuzzer}.dict").exists():
        custom_libfuzzer_options += f" -dict={fuzzer}.dict"
    custom_libfuzzer_options += f" -exact_artifact_path={exact_artifact_path}"

    custom_libfuzzer_options += f" -timeout={timeout}"

    libfuzzer_corpora = f"{active_corpus_dir} {seed_corpus_dir}"

    cmd_line = f"{DEBUGGER} ./{fuzzer} {fuzzer_arguments}"

    env = None
    with_fuzzer_args = ""
    if use_fuzzer_args:
        env = {"FUZZER_ARGS": f"{custom_libfuzzer_options} {libfuzzer_corpora}".strip()}
        with_fuzzer_args = f" with FUZZER_ARGS '{env['FUZZER_ARGS']}'"
    else:
        cmd_line += f" {custom_libfuzzer_options} {libfuzzer_corpora}"

    print(f"...will execute: '{cmd_line}'{with_fuzzer_args}")

    stopwatch = Utils.Stopwatch()
    try:
        with open(out_path, "wb") as out, open(stdout_path, "wb") as stdout:
            subprocess.run(
                cmd_line.split(),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=out,
                text=True,
                check=True,
                shell=False,
                errors="replace",
                timeout=timeout_hard,
                env=env,
            )
    except subprocess.CalledProcessError as e:
        print(f"Fail running {fuzzer}, ex {e}")
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"FAIL\n{stopwatch.start_time}\n{stopwatch.duration}\n"
            )
    except subprocess.TimeoutExpired:
        print(f"Successful running {fuzzer}")
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"OK\n{stopwatch.start_time}\n{stopwatch.duration}\n"
            )
    except Exception as e:
        print(f"Unexpected exception running {fuzzer}, ex {e}")
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"ERROR\n{stopwatch.start_time}\n{stopwatch.duration}\n"
            )
    else:
        assert False


def main():
    sw = Utils.Stopwatch()
    os.environ["CLICKHOUSE_BIN"] = "./ci/tmp/clickhouse"

    results = [Result.from_commands_run(name="Download corpus", command=download_corpus)]

    if results[-1].is_ok():
        results = [Result.from_commands_run(name="Run Fuzzers", command=["$CLICKHOUSE_BIN --version", run_fuzzer], workdir="./tests/fuzz")]

    Result.create_from(results=results, stopwatch=sw)


if __name__ == "__main__":
    main()
