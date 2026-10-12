#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e
WORK=$(mktemp -d "${CLICKHOUSE_TMP}/stderr_eof_XXXXXX")
trap 'rm -rf "$WORK"' EXIT

# The direct child exits after its row; its helper must not turn later EOF probes into
# new `command_read_timeout` waits. A noisy helper makes those waits exceed the query limit: a single
# one already does (30 s against 10 s), and the limit leaves room for starting Python under sanitizers.
cat > "$WORK/command.py" <<'PY'
#!/usr/bin/env python3
import os
import sys

ready_read, ready_write = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(1)
    os.close(ready_read)
    os.write(2, b"x" * 4096)
    os.write(ready_write, b"1")
    os.close(ready_write)
    try:
        while True:
            os.write(2, b"x" * 4096)
    except BrokenPipeError:
        os._exit(0)
os.close(ready_write)
with open(sys.argv[1], "w") as output:
    output.write(str(pid))
os.read(ready_read, 1)
os.close(ready_read)
os.write(1, b"row\n")
os._exit(0)
PY
chmod +x "$WORK/command.py"
python3 - "$CLICKHOUSE_LOCAL" "$WORK" <<'PY'
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys

work = Path(sys.argv[2])
pidfile = work / "helper.pid"
query = f"""
SELECT * FROM executable('command.py {pidfile}', TSV, 'value String',
    SETTINGS check_exit_code = 0, stderr_reaction = 'none',
        command_termination_timeout = 0, command_read_timeout = 30000)
SETTINGS max_threads = 1, max_execution_time = 10
"""
try:
    result = subprocess.run(
        shlex.split(sys.argv[1]) + ["--query", query, "--", "--user_scripts_path=" + str(work)],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0 and result.stdout == "row\n", result
    print("EOF does not wait for the helper")
finally:
    if pidfile.exists():
        try:
            os.kill(int(pidfile.read_text()), signal.SIGKILL)
        except ProcessLookupError:
            pass
PY
