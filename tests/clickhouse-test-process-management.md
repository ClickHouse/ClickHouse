# clickhouse-test process management

## Problem: orphan processes when the runner dies unexpectedly

`clickhouse-test` launches each `.sh` test in its own process group via
`start_new_session=True` (which sets `PGID = PID` for the spawned shell).
This is needed so that `os.killpg` can kill an entire test's subprocess tree
at once — bash does not forward signals to its children by default.

`cleanup_child_processes` handles graceful shutdown (SIGTERM/SIGINT/SIGHUP):
it walks direct children with `pgrep --parent` and calls `kill_process_group`
on each one.

If `clickhouse-test` or its parent `fast_test.py` is killed with **SIGKILL**
(e.g. OOM), those handlers never run.  The test subprocesses are re-parented
to `init`/`launchd` and keep running.  Because they are in separate sessions,
`pgrep --parent` no longer finds them.

On Linux the Docker container boundary kills everything when the container
exits.  On macOS Darwin CI (no Docker, no cgroups) the orphans accumulate
across test runs.

---

## Invariant: a per-test wrapper must not hold the runner's stdout or stderr

Because a surviving test process cannot be reaped in every case, it must at
least be **harmless**.  It is not harmless if it holds a descriptor belonging to
the runner's own stdout or stderr.  Only those two are redirected; fd 0 stays
inherited by design and cannot wedge anything, because holding a pipe's *read*
end does not prevent a downstream reader from seeing EOF, and a `.sh` test may
legitimately read the inherited stdin:

`ci/jobs/functional_tests.py` runs the runner as the head of a shell pipeline

```
set -o pipefail; clickhouse-test ... | ts '%Y-%m-%d %H:%M:%S' | tee -a "<file>"
```

so an orphan that inherited the runner's stdout keeps the write end of that pipe
open.  `ts` never sees EOF, `tee` never sees EOF, and **the pipeline never
finishes** even though the runner is long gone.  The job then runs to praktika's
wall-clock ceiling, and praktika then tears the job down: it signals the process
group it started (`TeePopen.send_signal` -> `os.killpg`) and runs `docker rm -f` on
the job's container.  Functional-test jobs run in a container
(`ci/defs/job_configs.py`), so that host process group holds the `docker run`
client, which forwards the signal into the container; either way
`ci/jobs/functional_tests.py` is killed while still blocked in `Shell.run`, before
it ever reaches `FTResultsProcessor.run(runner_exit_code=...)`, so the per-test
results are never processed at all: praktika finds an incomplete result, marks it
`ERROR` and fills `info` from its own rolling log buffer, and the run reports **no
per-test results at all**.  Note that the exit status itself is not the problem —
the signal-derived `-15` is already a member of `ABORTED_RUN_EXIT_CODES` in
`ci/jobs/scripts/functional_tests_results.py`; the processor simply never runs.

Therefore `run_single_test` gives the wrapper its own sink instead of letting it
inherit ours:

```python
open(self.stdout_file, "wb").close()          # see the fatal preexec_fn path below
open(self.stderr_file, "wb").close()          # start empty
with open(self.stderr_file, "ab") as wrapper_stderr:   # then append
    proc = Popen(command, shell=True, ..., start_new_session=True,
                 stdout=subprocess.DEVNULL, stderr=wrapper_stderr)
```

Nothing is lost: the test's own output is redirected by `command` itself
(`{test} > {stdout} 2> {stderr}`) and read back from those files by
`process_result_impl`.

Two producers can reach these streams:

* **The wrapper shell's own diagnostics** — the job-control messages bash emits *after*
  the redirect is installed, such as
  `line 1: 1234 Segmentation fault  <test> > ... 2> ...` when the wrapper's own child (the
  test command) dies from a signal, plus anything it writes when the redirect itself fails
  and so never truncates the file.  The exact shape of the job-control message depends on
  the signal: `SIGTERM` prints a bare `Terminated`, with no `line N:` prefix and no command
  echo.  When a background job the *test script* started is signalled instead, the message
  goes to the test's own stderr file as it always did, so this adds no new writer there and
  cannot turn `process_result_impl`'s "non-empty stderr" check into new failures.  These
  now land in the stderr file the harness already collects and reports, instead of the job
  log.
* **`preexec_fn`** (`setup_cgroup_with_memory_limit_cb`, used under `--memory-limit`),
  which runs in the child *after* `Popen` has installed these streams.  Its fatal
  `Failed to configure cgroup {name}: {e}` report reaches the stderr file rather
  than the job log, and it is written on the one path where bash never execs
  (the branch ends in `os._exit(1)`), so the command's own `2> {stderr}` never
  truncates it.  Because that same path also means `> {stdout}` never created the
  stdout file, `run_single_test` creates it before spawning — otherwise the
  post-run normalization, which opens that file unconditionally after `proc.wait`,
  would raise `FileNotFoundError` and the diagnostic would be reported as an
  unrelated internal error instead of as this test's failure.
  A write from a branch that *does* go on to exec would be truncated by the
  redirect, which is why the benign "cgroups are not available" notice is emitted
  from the parent instead: `report_cgroups_unavailable` is `functools.cache`d,
  so it reports once per test-executing process rather than once per spawn - up to
  `--jobs + 1` times, because the cache lives in the decorated function itself (so
  it is per process) and `run_single_test` is reached both from the forked parallel
  workers and directly from the parent for a suite's sequential tests.

Both halves of the `open` pair above are load-bearing, and for opposite reasons:

* **Truncate first**, because on the fatal `preexec_fn` path the redirect that
  would normally have emptied the file never runs, and `"Permission denied"` is
  one of `MESSAGES_TO_RETRY`, so without it each retried attempt would report
  every earlier attempt's diagnostics, and could carry a stale ` <Fatal> ` line,
  which `process_result_impl` promotes to `SERVER_DIED`.
* **Then append**, because the test command's own `2> {stderr}` opens the same
  path through an independent descriptor with its own offset.  A non-append
  descriptor here starts at 0, so the wrapper's job-control diagnostic would
  overwrite whatever the test already wrote to its stderr, including a
  ` <Fatal> ` line, silently disarming that same `SERVER_DIED` promotion.
  `O_APPEND` sends every write to the current end of the file, so the wrapper's
  diagnostics land after the test's own output.

**Scope.** This covers per-test wrappers and, transitively, their descendants
(they inherit from the wrapper).  It deliberately does **not** apply to the
parallel workers, which are forked `multiprocessing.Process` objects that inherit
the runner's stdout **by design**, because `run_tests_array` reports every
assembled result via `sys.stdout.write`.  The two `multiprocessing.Manager`
processes (one hosting the shared test queue, one the restarted-tests list) also
inherit it, but they are outside this invariant simply because they are not
per-test wrappers: they run no tests and print no results.
Consequently the invariant does not hold if the *top-level runner* is SIGKILL'd
(the surviving workers would then hold the pipe themselves); that is a separate
concern about worker lifetime.

**Regression coverage.**
`tests/queries/0_stateless/05218_clickhouse_test_wrapper_stdio_contract.sh` loads
`clickhouse-test` as a module and drives `run_single_test` with the `Popen` seam replaced, so it
checks all three properties above without needing a server and without timing dependence.

---

## Solution: PGID tracking via per-group pid files

The kernel stores the PGID directly in the process descriptor.  It is **never
reset** when a process is re-parented.  Therefore `kill_process_group(pgid)`
reaches an orphan as long as we know its PGID — no parent-chain walk needed.

### Group pid files

```
_GROUP_PID_PATH = {repo}/ci/tmp/
_GROUP_PID_NAME = "clickhouse_test_group_pid"
_RUN_TOKEN      = "<parent_pid>-<random hex>"   # seeded through the environment
                                               # (`_CLICKHOUSE_TEST_RUN_TOKEN`), so
                                               # `fork` and `spawn` workers alike share
                                               # the invocation's token
```

A worker process (`os.getpid()`) writes one file per group it launches:

```
{repo}/ci/tmp/clickhouse_test_group_pid.<run_token>.<worker_pid>.<pgid>
```

One PGID per file, and no two files ever share a name, so no cross-process
locking is needed and a record that is kept because its group may still be live
is not overwritten by that worker's next test.  The run token is what scopes a
reap to this invocation's own records (see `worker_pids` below), and it is passed
to workers through the environment because a `spawn` worker re-executes the
runner and would otherwise mint its own; a worker pid alone cannot scope a reap,
being recyclable into a concurrent invocation's worker.  Files are
written atomically via `write_text_atomic` (write to a `.tmp` sibling, then
`rename`), so `--cleanup` never sees a partial write.

### Per-test bookkeeping

```python
open(self.stdout_file, "wb").close()  # see the fatal preexec_fn path above
open(self.stderr_file, "wb").close()
with open(self.stderr_file, "ab") as wrapper_stderr:  # see the invariant above
    proc = Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn,
                 stdout=subprocess.DEVNULL, stderr=wrapper_stderr)
# proc.pid == PGID after start_new_session=True
write_text_atomic(test_process_group_record(proc.pid), f"{proc.pid}\n")

try:
    proc.wait(args.timeout)
finally:
    if cgroup_name:
        cleanup_cgroup(cgroup_name)
```

The record is written at launch and kept for as long as the group may still be
live, since it is the only thing that can lead a reaper to that group.
`process_result_impl` deals with the group once the test finishes and then calls
`forget_test_process_group`, but only if `test_process_group_is_gone` confirms
the group is gone, because neither `proc.returncode` nor a returning
`kill_process_group` rules out a backgrounded member that outlived the leader.
Anything still recorded is consumed by the parent's abort-path reap
(`reap_recorded_test_groups`) or by a later `--cleanup`.  So on a clean run no
files remain when `clickhouse-test` exits, and if a worker is SIGKILL'd its
record survives for those reapers.

### `--cleanup` mode

```
clickhouse-test --cleanup
```

Calls `cleanup_test_groups()`, which globs `{_GROUP_PID_PATH}/{_GROUP_PID_NAME}.*`
(skipping `.tmp` files), reads each file, calls `kill_process_group(pgid, None)`
on the recorded PGID, and removes the file.  It passes no `worker_pids`, so it
matches every record regardless of name shape: it is an orphan sweep run when
nothing else is live, and the orphan it exists for may predate a name change.

### `clickhouse-test` startup

```python
# Move to a new process group so terminal signals don't reach our caller.
# If the caller already used start_new_session=True we are already a process
# group leader and setpgid would raise PermissionError — that is fine.
if os.getpid() != os.getpgid(0):
    os.setpgid(0, 0)
```

### Caller cleanup (`run_test` in `clickhouse_proc.py`)

```python
# in finally block after process.wait():
subprocess.run([sys.executable, str(_clickhouse_test), "--cleanup"], check=False)
```

### Post-hook guard

`ci/defs/job_configs.py` registers the hook for post-execution:

```python
darwin_fast_test_jobs = Job.Config(
    ...
    post_hooks=["python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py"],
)
```

The **post-hook** covers the case where `fast_test.py` itself is SIGKILL'd and
the `finally` block in `run_test` never executes.

The hook contains no kill logic of its own — it just calls
`clickhouse-test --cleanup`, the single source of truth for orphan cleanup.

### Cleanup layers

| Layer | Trigger | Mechanism |
|---|---|---|
| `cleanup_child_processes` | SIGTERM/SIGINT/SIGHUP to `clickhouse-test` | `killpg` on each direct child's PGID (so it cannot reach a group whose leader already exited) |
| `process_result_impl` | The test finished | `kill_process_group`, then `forget_test_process_group` if the group is gone |
| `reap_recorded_test_groups` | Any exit of the parallel or sequential runner: an abort (hung check, server death, time limit, `--max-failures`), a signal to the parent, or a normal finish | `kill_process_group` per record written by this run's own workers |
| `run_test` `finally` | Any exit of `clickhouse-test` (incl. SIGKILL) | `clickhouse-test --cleanup` → `kill_process_group` per PGID file |
| Post-hook | Any exit of `fast_test.py` (incl. SIGKILL) | same — `clickhouse-test --cleanup` |

The abort-path reap is pinned by `tests/integration/test_clickhouse_test_abort_reap`:
token and worker scoping, `spawn` propagation of the token, the signal masking of
`reap_recorded_test_groups` and `quiesce_workers_and_reap`, and stopping the workers
before the single walk over the records.

### Remaining limitation

If `runner.py` itself is killed before the post-hook executes, nothing cleans
up.  On a dedicated macOS runner this requires a machine-level failure; a reboot
clears all processes.  For Linux production CI the Docker boundary already covers this.

---

## Known issues

### Process group not killed on normal test exit

When the bash script exits normally (exit code is set), `kill_process_group` is
**not** called:

```python
# process_result_impl
timed_out = proc.returncode is None      # only true on TimeoutExpired
if timed_out:
    kill_process_group(proc.pid, ...)
elif test_process_group_is_gone(proc.pid):
    forget_test_process_group(proc.pid)
```

So background jobs the test script started without `wait` are still not killed
here, to avoid the overhead on every normally passing test.  They are no longer
unreachable, though: the group is only forgotten once it is actually gone, so a
surviving member keeps its record and stays reapable by the abort-path reap and
by `--cleanup`.

In practice most shell tests call `wait` at the end, so the group is empty by
the time bash exits and the record is dropped immediately.
