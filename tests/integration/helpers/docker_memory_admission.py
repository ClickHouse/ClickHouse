"""Memory admission of cluster starts into the `/docker` cgroup leaf of `docker_in_docker.sh`.

Enabled by `CLICKHOUSE_TESTS_DOCKER_MEMORY_PER_PROCESS` (bytes per ClickHouse process), which the CI
job sets. A cluster waits before creating its containers while the leaf would be over `THRESHOLD`
of its cap with it. The xdist workers of a session share a registry of the clusters that were
admitted but have not settled yet, and of the ones waiting, which are admitted in arrival order.
"""

import json
import logging
import os
import time

from filelock import FileLock

DOCKER_CGROUP = "/sys/fs/cgroup/docker"
REGISTRY = "/tmp/docker_memory_admission.json"
LOCK = "/tmp/docker_memory_admission.lock"
PER_PROCESS_ENV = "CLICKHOUSE_TESTS_DOCKER_MEMORY_PER_PROCESS"
THRESHOLD = 0.8
MAX_WAIT = 450
POLL = 5
SETTLE = 120
ADMITTED_FOR = 900
FIELDS = {"id", "pid", "bytes", "state", "since", "expires"}

GIB = 1024**3


def _per_process():
    value = os.environ.get(PER_PROCESS_ENV, "")
    if not value:
        return None
    if not value.isascii() or not value.isdigit() or int(value) <= 0:
        raise ValueError(f"{PER_PROCESS_ENV} is [{value}], expected a positive byte count")
    return int(value)


def _read(name):
    path = os.path.join(DOCKER_CGROUP, name)
    try:
        with open(path, encoding="utf-8") as f:
            return path, f.read()
    except OSError as e:
        raise RuntimeError(f"Cannot read {path}, needed because {PER_PROCESS_ENV} is set: {e}")


def _leaf():
    """`(cap, used)` of the leaf; `used` leaves out the page cache, which is reclaimable."""
    path, text = _read("memory.max")
    if not text.strip().isdigit():
        raise RuntimeError(f"{path} is [{text.strip()}], {PER_PROCESS_ENV} needs a capped leaf")
    cap = int(text)
    path, text = _read("memory.current")
    if not text.strip().isdigit():
        raise RuntimeError(f"{path} is [{text.strip()}], expected a byte count")
    path, stat = _read("memory.stat")
    fields = dict(line.split() for line in stat.splitlines() if len(line.split()) == 2)
    if not all(fields.get(k, "").isdigit() for k in ("active_file", "inactive_file")):
        raise RuntimeError(f"{path} has no active_file and inactive_file")
    return cap, int(text) - int(fields["active_file"]) - int(fields["inactive_file"])


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def _load(now):
    try:
        with open(REGISTRY, encoding="utf-8") as f:
            entries = json.load(f)
        if not all(isinstance(e, dict) and e.keys() == FIELDS for e in entries):
            raise ValueError(f"expected entries with the fields {sorted(FIELDS)}")
        return [e for e in entries if e["expires"] > now and _alive(e["pid"])]
    except FileNotFoundError:
        return []
    except (OSError, ValueError, TypeError) as e:
        raise RuntimeError(f"Cannot parse {REGISTRY}: {e!r}")


def _save(entries):
    tmp = f"{REGISTRY}.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(entries, f)
    os.replace(tmp, REGISTRY)


def reserve(name, processes):
    """Wait until `processes` ClickHouse processes of cluster `name` fit, or raise after `MAX_WAIT`."""
    per_process = _per_process()
    if per_process is None or processes == 0:
        return
    own = f"{os.getpid()}:{name}"
    need = processes * per_process
    since = time.time()
    logged = False
    while True:
        with FileLock(LOCK):
            now = time.time()
            entries = [e for e in _load(now) if e["id"] != own]
            ahead = sum(
                e["bytes"]
                for e in entries
                if e["state"] == "admitted" or (e["since"], e["id"]) < (since, own)
            )
            cap, used = _leaf()
            limit = THRESHOLD * cap
            numbers = (
                f"{used / GIB:.1f} GiB in use + {ahead / GIB:.1f} GiB ahead + {need / GIB:.1f} GiB "
                f"for {processes} ClickHouse processes"
            )
            entry = {"id": own, "pid": os.getpid(), "bytes": need, "since": since}
            if used + ahead + need <= limit:
                _save(entries + [dict(entry, state="admitted", expires=now + ADMITTED_FOR)])
                if logged:
                    logging.info(f"Memory in /docker for {name} admitted after {now - since:.0f} s: {numbers}")
                else:
                    logging.debug(f"Memory in /docker for {name}: {numbers} <= {limit / GIB:.1f} GiB")
                return
            over = f"{numbers} > {limit / GIB:.1f} GiB ({THRESHOLD:.0%} of {cap / GIB:.1f} GiB)"
            if now >= since + MAX_WAIT:
                _save(entries)
                raise RuntimeError(f"Not enough memory in /docker to start {name} after {MAX_WAIT} s: {over}")
            _save(entries + [dict(entry, state="waiting", expires=since + MAX_WAIT + POLL)])
        if not logged:
            logging.info(f"Waiting for memory in /docker before starting {name}: {over}")
            logged = True
        time.sleep(POLL)


def _update(name, expires):
    """Set the `expires` of the own entry, or drop it when `expires` is None. Never raises."""
    if not os.environ.get(PER_PROCESS_ENV):
        return
    own = f"{os.getpid()}:{name}"
    try:
        with FileLock(LOCK):
            entries = _load(time.time())
            if not any(e["id"] == own for e in entries):
                return
            if expires is None:
                _save([e for e in entries if e["id"] != own])
            else:
                _save([dict(e, expires=expires) if e["id"] == own else e for e in entries])
    except (OSError, RuntimeError) as e:
        logging.warning(f"Cannot update the /docker memory admission of {name}: {e}")


def settle(name):
    """The cluster is up: keep its estimate for `SETTLE` more seconds while its servers grow."""
    _update(name, time.time() + SETTLE)


def release(name):
    _update(name, None)
