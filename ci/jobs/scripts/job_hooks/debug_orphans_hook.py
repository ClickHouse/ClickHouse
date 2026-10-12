"""Post-hook: report and kill processes that outlived the job, and say why cleanup missed them.

Runs after `clickhouse_test_cleanup_hook.py`. A survivor here is a process whose parent
died (reparented to PID 1) and that still references the job's checkout or is a
ClickHouse binary. For each one it prints the evidence that explains why neither
`ClickHouseProc.stop_server` nor `clickhouse-test --cleanup` killed it, then sends
SIGTERM and, after a grace period, SIGKILL.
"""

import os
import pwd
import re
import signal
import subprocess
import time
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent.parent.parent
GRACE_SECONDS = 10
CLICKHOUSE_DAEMON = re.compile(r"clickhouse[- ](server|watchd|watchdog)")


def processes():
    out = subprocess.run(
        ["ps", "-axww", "-o", "pid=,ppid=,pgid=,stat=,etime=,rss=,user=,command="],
        capture_output=True, text=True, check=True,
    ).stdout
    procs = {}
    for line in out.splitlines():
        pid, ppid, pgid, stat, etime, rss, user, command = line.split(None, 7)
        procs[int(pid)] = dict(
            pid=int(pid), ppid=int(ppid), pgid=int(pgid), stat=stat,
            etime=etime, rss_mib=int(rss) // 1024, user=user, command=command,
        )
    return procs


def is_orphan(p):
    return (
        p["ppid"] == 1
        and p["pid"] != os.getpid()
        and p["user"] == pwd.getpwuid(os.getuid()).pw_name
        and (str(repo_path) in p["command"] or CLICKHOUSE_DAEMON.search(p["command"]))
    )


def why_missed(p, procs):
    reasons = []
    if CLICKHOUSE_DAEMON.search(p["command"]):
        reasons.append(
            "ClickHouse server started by `ClickHouseProc`, outside clickhouse-test's recorded groups,"
            " so `--cleanup` never targets it; with ppid 1 its watchdog parent died first"
            " (macOS has no `PR_SET_PDEATHSIG`, so the server is not killed with it)"
        )
    if p["pgid"] == p["pid"]:
        reasons.append("leads its own process group (called `setsid`/`setpgid`), so a `killpg` of its test's group missed it")
    elif p["pgid"] not in procs:
        reasons.append(f"the leader of its process group {p['pgid']} has exited, so the group outlived the process that owned it")
    if "U" in p["stat"] or "D" in p["stat"]:
        reasons.append(f"state `{p['stat']}` is uninterruptible, so signals wait until the kernel call returns")
    test = re.search(r"tests/queries/[^/ ]+/([^/ .]+)", p["command"])
    if test:
        reasons.append(f"started by test `{test.group(1)}`")
    return reasons or ["no known reason; see the command line"]


def main():
    procs = processes()
    orphans = [p for p in procs.values() if is_orphan(p)]
    if not orphans:
        print("debug_orphans: no orphaned processes")
        return

    print(f"debug_orphans: {len(orphans)} orphaned process(es) survived the job's cleanup")
    for p in orphans:
        print(f"  pid={p['pid']} pgid={p['pgid']} stat={p['stat']} etime={p['etime']} rss={p['rss_mib']}MiB")
        print(f"    command: {p['command'][:300]}")
        for reason in why_missed(p, procs):
            print(f"    why: {reason}")

    def alive():
        # Same pid and command, so a recycled pid is never signalled.
        now = processes()
        return [p for p in orphans if now.get(p["pid"], {}).get("command") == p["command"]]

    for sig in (signal.SIGTERM, signal.SIGKILL):
        for p in alive():
            try:
                os.kill(p["pid"], sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + GRACE_SECONDS
        while time.monotonic() < deadline and alive():
            time.sleep(1)

    survivors = alive()
    for p in survivors:
        print(f"debug_orphans: pid {p['pid']} survived SIGKILL: {p['command'][:200]}")
    print(f"debug_orphans: killed {len(orphans) - len(survivors)} of {len(orphans)}")


main()
