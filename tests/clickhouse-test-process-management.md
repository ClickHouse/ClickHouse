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

## Solution: PGID tracking via per-worker group pid files

The kernel stores the PGID directly in the process descriptor.  It is **never
reset** when a process is re-parented.  Therefore `kill_process_group(pgid)`
reaches an orphan as long as we know its PGID — no parent-chain walk needed.

### Group pid files

```
_GROUP_PID_PATH = {repo}/ci/tmp/
_GROUP_PID_NAME = "clickhouse_test_group_pid"
```

Each worker process (`os.getpid()`) writes its own file:

```
{repo}/ci/tmp/clickhouse_test_group_pid.<worker_pid>
```

One PGID per file.  Because every worker owns a separate file no cross-process
locking is needed.  Files are written atomically via `write_text_atomic`
(write to a `.tmp` sibling, then `rename`), so `--cleanup` never sees a
partial write.

### Per-test bookkeeping

```python
proc = Popen(command, shell=True, start_new_session=True, preexec_fn=cgroup_fn)
# proc.pid == PGID after start_new_session=True
_gpid_file = _GROUP_PID_PATH / f"{_GROUP_PID_NAME}.{os.getpid()}"
write_text_atomic(_gpid_file, f"{proc.pid}\n")

try:
    proc.wait(args.timeout)
finally:
    if cgroup_name:
        cleanup_cgroup(cgroup_name)
    _gpid_file.unlink(missing_ok=True)
```

On a clean run every started test deletes its file in the `finally` block, so
no files remain when `clickhouse-test` exits.  If `clickhouse-test` is
SIGKILL'd, the file for the currently-running test is left behind with its
PGID.

### `--cleanup` mode

```
clickhouse-test --cleanup
```

Calls `cleanup_test_groups()`, which globs `{_GROUP_PID_PATH}/{_GROUP_PID_NAME}.*`
(skipping `.tmp` files), reads each file, calls `kill_process_group(pgid, None)`
on the recorded PGID, and removes the file.

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
| `cleanup_child_processes` | SIGTERM/SIGINT/SIGHUP to `clickhouse-test` | `killpg` on each direct child's PGID |
| test `finally` block | Any exit of the per-test code path (incl. SIGKILL to the worker) | `_gpid_file.unlink` — removes the per-worker file |
| `run_test()` `finally` | Any exit of `clickhouse-test` (incl. SIGKILL) | `clickhouse-test --cleanup` → `kill_process_group` per PGID file |
| Post-hook | Any exit of `fast_test.py` (incl. SIGKILL) | same — `clickhouse-test --cleanup` |

### Remaining limitation

If `runner.py` itself is killed before the post-hook executes, nothing cleans
up.  On a dedicated macOS runner this requires a machine-level failure; a reboot
clears all processes.  For Linux production CI the Docker boundary already covers this.

---

## Known issues

### Process group not killed on normal test exit

When the bash script exits normally (exit code is set), `kill_process_group` is
**not** called.  The code path is:

```python
# run_single_test_command (line ~3136)
proc = Popen(command, shell=True, start_new_session=True, ...)
_gpid_file = _GROUP_PID_PATH / f"{_GROUP_PID_NAME}.{os.getpid()}"
write_text_atomic(_gpid_file, f"{proc.pid}\n")
try:
    proc.wait(args.timeout)
except subprocess.TimeoutExpired:
    pass
finally:
    _gpid_file.unlink(missing_ok=True)   # file removed here on every exit
return proc, total_time

# process_result_impl (line ~2712)
if proc.returncode is None:              # only true on TimeoutExpired
    kill_process_group(os.getpgid(proc.pid), ...)
```

Consequence: any processes that are still in the process group after bash exits
(e.g. background jobs the test script started without `wait`) are **not killed**
and the PGID file is already gone, so `--cleanup` cannot reach them either.

In practice, most shell tests call `wait` at the end, so all background jobs
finish before bash exits and the group is empty.  A test that does not call
`wait` (or that spawns detached sub-subprocesses inside the group) leaks those
processes silently.

The fix would be to call `kill_process_group` on the PGID before deleting the
file, unconditionally (or at least when `pgrep(pgid=proc.pid)` still shows
living members).  This is not done today to avoid the overhead on every normally
passing test.
