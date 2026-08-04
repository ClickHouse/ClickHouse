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

## Solution: PGID tracking via per-group pid files

The kernel stores the PGID directly in the process descriptor.  It is **never
reset** when a process is re-parented.  Therefore `kill_process_group(pgid)`
reaches an orphan as long as we know its PGID — no parent-chain walk needed.

### Group pid files

```
_GROUP_PID_PATH = {repo}/ci/tmp/
_GROUP_PID_NAME = "clickhouse_test_group_pid"
_RUN_TOKEN      = "<parent_pid>-<random hex>"   # seeded through the environment, so
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
proc = Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn)
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
    kill_process_group(os.getpgid(proc.pid), ...)
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
