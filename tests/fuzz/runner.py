#!/usr/bin/env python3

import configparser
import datetime
import logging
import os
import re
import shutil
import subprocess
import psutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import signal

DEBUGGER = os.getenv("DEBUGGER", "")
TIMEOUT = int(os.getenv("TIMEOUT", "0"))
OUTPUT = "/test_output"
RUNNERS = int(os.getenv("RUNNERS", "16"))
DEFAULT_INPUT_TIMEOUT = 1200 # libFuzzer default value for '-timeout' option

INPUT_TIMEOUT = 0 # for debugging, set 0 when not debugging

# Run merge fuzzer process with timeout. On timeout send SIGUSR1 graceful termination signal.
# If process does not exit, SIGKILL is issued after additional kill_timeout time.
# If process exits on SIGUSR1 signal it is treated as a normal exit.
# If process termination is a result of the SIGKILL signal then subprocess.TimeoutExpired is raised.
# Fuzzer merge starts a child process, so to gracefully terminate fuzzer we need to send SIGUSR1 signal
# to this child instead of parent (seems to be some kind of bug in libFuzzer)
def run_merge_fuzzer(*popenargs,
        input=None, capture_output=False, timeout=None, check=False, kill_timeout=10, **kwargs):
    if input is not None:
        if kwargs.get('stdin') is not None:
            raise ValueError('stdin and input arguments may not both be used.')
        kwargs['stdin'] = PIPE

    if capture_output:
        if kwargs.get('stdout') is not None or kwargs.get('stderr') is not None:
            raise ValueError('stdout and stderr arguments may not be used '
                             'with capture_output.')
        kwargs['stdout'] = PIPE
        kwargs['stderr'] = PIPE

    stdout = None
    stderr = None
    with subprocess.Popen(*popenargs, **kwargs) as process:
        try:
            stdout, stderr = process.communicate(input, timeout=timeout)
        except subprocess.TimeoutExpired:
            current_process = psutil.Process(process.pid)
            for child in current_process.children(recursive=True):
                if child.name() == current_process.name():
                    os.kill(child.pid, signal.SIGUSR1)
                    break
            try:
                process.wait(timeout=kill_timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise
        except:  # Including KeyboardInterrupt, communicate handled that.
            process.kill()
            # We don't call process.wait() as .__exit__ does that for us.
            raise
        retcode = process.poll()
        if check and retcode:
            raise subprocess.CalledProcessError(retcode, process.args,
                                     output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(process.args, retcode, stdout, stderr)

class Stopwatch:
    def __init__(self):
        self.reset()

    @property
    def duration_seconds(self) -> float:
        return (
            datetime.datetime.now(datetime.timezone.utc) - self.start_time
        ).total_seconds()

    @property
    def start_time_str(self) -> str:
        return self.start_time_str_value

    def reset(self) -> None:
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.start_time_str_value = self.start_time.strftime("%Y-%m-%d %H:%M:%S")


def run_fuzzer(fuzzer: str, timeout: int):
    
    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_present = True
    active_corpus_dir = f"corpus/{fuzzer}"
    active_corpus_path = Path(active_corpus_dir)
    if not active_corpus_path.exists() or len(list(active_corpus_path.iterdir())) == 0:
        active_corpus_present = False
        active_corpus_path.mkdir(parents=True, exist_ok=True)
    mini_corpus_dir = f"corpus/{fuzzer}_mini"
    if not os.path.exists(mini_corpus_dir):
        os.makedirs(mini_corpus_dir)
    merge_control_file = f"{fuzzer}_merge_control.txt"
    options_file = f"{fuzzer}.options"
    # custom arguments for fuzzer executable
    fuzzer_arguments = ""
    # libFuzzer options
    libfuzzer_options = ""
    libfuzzer_merge_options = ""
    # libFuzzer options which are allowed in user's .options in the "libfuzzer" section
    allowed_libfuzzer_options = [
        "seed",                 # Random seed. If 0 (the default), the seed is generated.
        "max_len",              # Maximum length of a test input. If 0 (the default), libFuzzer tries to guess a good value based on the corpus.
        "len_control",          # Try generating small inputs first, then try larger inputs over time. Specifies the rate at which the length limit
                                # is increased (smaller == faster). Default is 100. If 0, immediately try inputs with size up to max_len.
        "timeout",              # Timeout in seconds, default 1200. If an input takes longer than this timeout, the process is treated as a failure case.
        "report_slow_units",    # Not a standard documented flag. Report slowest units if they run for more than this number of seconds. Current default is 10.
        "rss_limit_mb",         # Memory usage limit in Mb, default 2048. Use 0 to disable the limit. If an input requires more than this amount
                                # of RSS memory to execute, the process is treated as a failure case. The limit is checked in a separate thread every second.
        "malloc_limit_mb",      # If non-zero, the fuzzer will exit if the target tries to allocate this number of Mb with one malloc call. If zero (default)
                                # same limit as rss_limit_mb is applied.
        "only_ascii",           # If 1, generate only ASCII (isprint``+``isspace) inputs. Defaults to 0.
        "dict",                 # Provide a dictionary of input keywords, if absent fuzzer specific dictionary with name <fuzzer>.dict is used when present.
    ]

    allowed_merge_libfuzzer_options = [
        "timeout",              # Timeout in seconds, default 1200. If an input takes longer than this timeout, the process is treated as a failure case.
        "report_slow_units",    # Not a standard documented flag. Report slowest units if they run for more than this number of seconds. Current default is 10.
        "rss_limit_mb",         # Memory usage limit in Mb, default 2048. Use 0 to disable the limit. If an input requires more than this amount
                                # of RSS memory to execute, the process is treated as a failure case. The limit is checked in a separate thread every second.
        "malloc_limit_mb",      # If non-zero, the fuzzer will exit if the target tries to allocate this number of Mb with one malloc call. If zero (default)
                                # same limit as rss_limit_mb is applied.
    ]

    input_timeout = 0

    if INPUT_TIMEOUT:
        allowed_libfuzzer_options.remove("timeout")
        allowed_merge_libfuzzer_options.remove("timeout")
        input_timeout = INPUT_TIMEOUT

    use_fuzzer_args = False

    env = {}

    with Path(options_file) as path:
        if path.exists() and path.is_file():
            parser = configparser.ConfigParser()
            parser.read(path)

            if parser.has_section("asan"):
                env["ASAN_OPTIONS"] = (
                    f"{os.environ['ASAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['asan'].items())}"
                )

            if parser.has_section("msan"):
                env["MSAN_OPTIONS"] = (
                    f"{os.environ['MSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['msan'].items())}"
                )

            if parser.has_section("ubsan"):
                env["UBSAN_OPTIONS"] = (
                    f"{os.environ['UBSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['ubsan'].items())}"
                )

            if parser.has_section("fuzzer_arguments"):
                fuzzer_arguments = " ".join(
                    (f"{key}") if value == "" else (f"{key}={value}")
                    for key, value in parser["fuzzer_arguments"].items()
                )

            if parser.has_section("libfuzzer"):
                libfuzzer_options = " ".join(
                    f"-{key}={value}"
                    for key, value in parser["libfuzzer"].items()
                    if key in allowed_libfuzzer_options
                )
                libfuzzer_merge_options = " ".join(
                    f"-{key}={value}"
                    for key, value in parser["libfuzzer"].items()
                    if key in allowed_merge_libfuzzer_options
                )
                input_timeout = parser["libfuzzer"].getint("timeout", fallback=input_timeout)

            # FUZZER_ARGS flag is used to make it deliver libFuzzer arguments throught FUZZER_ARGS environment variable
            # for special cases of fuzzers written in the way they don't use libFuzzer framework, but rather
            # implement their own main (usually it's a whole application which implements fuzzer functionality alongside)
            # and then initialize libFuzzer driver themselves. Such approach allows fuzzer executable to process its
            # arguments as usual, without any special measures, but initialization of libFuzer driver then should take
            # arguments from FUZZER_ARGS environment variable.
            use_fuzzer_args = parser.getboolean("CI", "FUZZER_ARGS", fallback=False)

    if INPUT_TIMEOUT:
        libfuzzer_options += f" -timeout={INPUT_TIMEOUT}"
        libfuzzer_merge_options += f" -timeout={INPUT_TIMEOUT}"

    results_path = f"{OUTPUT}/{fuzzer}.results/"
    if not os.path.exists(results_path):
        os.makedirs(results_path)
    artifact_prefix = f"{results_path}"

    # Corpus minimization
    if active_corpus_present:
        logging.info(
            "Running corpus minimization for fuzzer %s for %d seconds...",
            fuzzer,
            timeout,
        )

        merge_libfuzzer_options = f" {libfuzzer_merge_options} -artifact_prefix={artifact_prefix}mini- -merge=1 -max_total_time={timeout} -merge_control_file={merge_control_file} {mini_corpus_dir} {active_corpus_dir}"
        cmd_line = f"{DEBUGGER} ./{fuzzer}"

        with_fuzzer_args = ""
        if use_fuzzer_args:
            env["FUZZER_ARGS"] = f"{merge_libfuzzer_options}".strip()
            with_fuzzer_args = f" with FUZZER_ARGS '{env['FUZZER_ARGS']}'"
        else:
            cmd_line += f" {merge_libfuzzer_options}"
            if fuzzer_arguments:
                cmd_line += " -ignore_remaining_args=1"

        if fuzzer_arguments:
            cmd_line += f" {fuzzer_arguments}"

        logging.info("...will execute corpus minimization: '%s'%s", cmd_line, with_fuzzer_args)

        status_path = f"{results_path}/status_mini.txt"
        out_path = f"{results_path}/out_mini.txt"
        stdout_path = f"{results_path}/stdout_mini.txt"

        merge_ok = True
        stopwatch = Stopwatch()
        try:
            with open(out_path, "wb") as out, open(stdout_path, "wb") as stdout:
                run_merge_fuzzer(
                    cmd_line.split(),
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=out,
                    text=True,
                    check=True,
                    shell=False,
                    errors="replace",
                    timeout=timeout,
                    kill_timeout= input_timeout * 2 if input_timeout > 0 else DEFAULT_INPUT_TIMEOUT,
                    env=env,
                )
        except subprocess.CalledProcessError as e:
            logging.info("Unexpected termination while running corpus minimization %s: %s", fuzzer, e)
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )
            merge_ok = False
        except subprocess.TimeoutExpired:
            logging.info("Unexpected timeout while finishing corpus minimization %s", fuzzer)
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )
            merge_ok = False
        except Exception as e:
            logging.info("Unexpected exception while running corpus minimization %s: %s", fuzzer, e)
            traceback.print_exc()
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )
            merge_ok = False

        # lack of merge_control_file is an indication that something went wrong
        if not os.path.exists(merge_control_file):
            logging.info("Unexpected absence of merge_control_file while running corpus minimization %s", fuzzer)
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )
            merge_ok = False

        # empty destination corpus is an indication that something went wrong
        if len(list(Path(mini_corpus_dir).glob("*"))) == 0:
            logging.info("Unexpected empty destination corpus while running corpus minimization %s", fuzzer)
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )
            merge_ok = False

        if merge_ok:
            with open(status_path, "w", encoding="utf-8") as status:
                status.write(
                    f"OK\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
                )

            orig_corpus_size = len(list(Path(active_corpus_dir).glob("*")))

            # Remove processed files from original corpus
            all_files = []
            processed_files = []
            with open(merge_control_file, "r", encoding="utf-8") as f:
                INDEX_LINE = r"^STARTED\s+(\d+)\s+\d+"
                for i, line in enumerate(f):
                    if len(processed_files) == 0:
                        if i < 2:
                            continue
                    line = line.strip()
                    match = re.search(INDEX_LINE, line)
                    if match:
                        processed_files.append(all_files[int(match.group(1))])
                    if len(processed_files) == 0:
                        all_files.append(line)

            for fname in processed_files:
                orig_file = Path(fname)
                if orig_file.exists():
                    orig_file.unlink()

            not_processed_size = len(list(Path(active_corpus_dir).glob("*")))

            # Copy minimized corpus files back to original corpus
            for file in Path(mini_corpus_dir).iterdir():
                if file.is_file():
                    shutil.copy2(file, Path(active_corpus_dir) / file.name)

            # Delete minimized corpus directory
            shutil.rmtree(mini_corpus_dir)

            mini_corpus_size = len(list(Path(active_corpus_dir).glob("*")))

            reduction = 0
            if orig_corpus_size > 0:
                reduction = mini_corpus_size * 100 / orig_corpus_size

            logging.info("Successful run, corpus minimization for %s, original corpus size %d, processed %d, not processed %d, minimized size %d, reduced to %d%%",
                fuzzer, orig_corpus_size, len(processed_files), not_processed_size, mini_corpus_size, reduction)
        else:
            # Delete minimized corpus directory
            shutil.rmtree(mini_corpus_dir)
    else:
        logging.info("Not running corpus minimization for fuzzer %s - persistent corpus is empty", fuzzer)


    # Fuzzing
    logging.info(
        "Running fuzzer %s for %d seconds...",
        fuzzer,
        timeout,
    )

    status_path = f"{results_path}/status.txt"
    out_path = f"{results_path}/out.txt"
    stdout_path = f"{results_path}/stdout.txt"

    if not "-dict=" in libfuzzer_options and Path(f"{fuzzer}.dict").exists():
        libfuzzer_options += f" -dict={fuzzer}.dict"
    libfuzzer_options += f" -artifact_prefix={artifact_prefix}"

    libfuzzer_corpora = f"{active_corpus_dir} {seed_corpus_dir}"

    cmd_line = f"{DEBUGGER} ./{fuzzer}"

    with_fuzzer_args = ""
    if use_fuzzer_args:
        env["FUZZER_ARGS"] = f"{libfuzzer_options} {libfuzzer_corpora}".strip()
        with_fuzzer_args = f" with FUZZER_ARGS '{env['FUZZER_ARGS']}'"
    else:
        cmd_line += f" {libfuzzer_options} {libfuzzer_corpora}"
        if fuzzer_arguments:
            cmd_line += " -ignore_remaining_args=1"

    if fuzzer_arguments:
        cmd_line += f" {fuzzer_arguments}"

    logging.info("...will execute: '%s'%s", cmd_line, with_fuzzer_args)

    stopwatch = Stopwatch()
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
                timeout=timeout,
                env=env,
            )
    except subprocess.CalledProcessError:
        logging.info("Fail found while running %s", fuzzer)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"FAIL\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
    except subprocess.TimeoutExpired:
        logging.info("Successful running %s", fuzzer)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"OK\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
    except Exception as e:
        logging.info("Unexpected exception running %s: %s", fuzzer, e)
        traceback.print_exc()
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
    else:
        logging.info("Unexpected exit while running %s", fuzzer)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"ERROR\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )


def main():
    logging.basicConfig(level=logging.INFO)

    subprocess.check_call("ls -al", shell=True)

    timeout = 30 if TIMEOUT == 0 else TIMEOUT

    current = Path(".")
    with ThreadPoolExecutor(max_workers=RUNNERS) as executor:
        futures = {}
        for fuzzer in current.iterdir():
            if fuzzer.is_file() and os.access(fuzzer, os.X_OK):
                futures[executor.submit(run_fuzzer, fuzzer.name, timeout)] = fuzzer.name

        for future in as_completed(futures):
            fuzzer = futures[future]
            try:
                result = future.result()
                logging.info("Thread for %s finished", fuzzer)
            except Exception as exc:
                logging.info("Thread for %s generated an exception: %s", fuzzer, exc)
                traceback.print_exc()

    subprocess.check_call(f"ls -al {OUTPUT}", shell=True)


if __name__ == "__main__":
    main()
