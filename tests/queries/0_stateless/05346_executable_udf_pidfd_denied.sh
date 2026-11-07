#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: the regression requires Linux seccomp and pidfd

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
WORK=$(mktemp -d "${CLICKHOUSE_TMP}/pidfd_denied_XXXXXX")
trap 'rm -rf "$WORK"' EXIT
printf '#!/usr/bin/env bash\necho row\nexec 1>&- 2>&-\nsleep 2\n' > "$WORK/command.sh"
chmod +x "$WORK/command.sh"

# A denial of `pidfd_open` (a seccomp profile of an older container runtime) is not an error of the
# command: the wait for its exit goes on in short steps, as on a kernel without `pidfd`. The command
# closes its stdout and stderr before it exits, so the wait gets to the point where only the exit is
# left to wait for.
python3 - "$CLICKHOUSE_LOCAL" "$WORK" <<'PY'
import ctypes
import errno
import os
import platform
import re
import shlex
import subprocess
import sys


# Older kernels use a different wait path and cannot exercise this regression.
version = tuple(map(int, re.match(r"(\d+)\.(\d+)", platform.release()).groups()))
if version < (5, 3):
    print("@@SKIP@@: the regression requires Linux 5.3 or newer")
    sys.exit(0)


class Filter(ctypes.Structure):
    _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte), ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint)]


class Program(ctypes.Structure):
    _fields_ = [("length", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]


def deny_pidfd():
    libc = ctypes.CDLL(None, use_errno=True)
    # Linux syscall number, matching `syscall_pidfd_open`.
    number = 206 if platform.machine() == "e2k" else 434
    filters = (Filter * 4)(
        Filter(0x20, 0, 0, 0),
        Filter(0x15, 0, 1, number),
        Filter(0x06, 0, 0, 0x00050000 | errno.EPERM),
        Filter(0x06, 0, 0, 0x7FFF0000),
    )
    program = Program(4, filters)
    if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        os._exit(120)


query = """
SELECT * FROM executable('command.sh', TSV, 'value String',
    SETTINGS check_exit_code = 1, stderr_reaction = 'none') SETTINGS max_threads = 1
"""
result = subprocess.run(
    shlex.split(sys.argv[1]) + ["--query", query, "--", "--user_scripts_path=" + sys.argv[2]],
    preexec_fn=deny_pidfd, capture_output=True, text=True, timeout=30,
)
assert result.returncode == 0 and result.stdout == "row\n", result
print("waited without pidfd")
PY
