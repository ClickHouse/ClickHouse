"""Exit with an error unless the running ClickHouse servers with the given data paths have their
executable loaded at the same address. Usage: check_executable_base.py <data path> <data path>..."""

import os
import re
import sys


def executable_base(data_path):
    with open(f"{data_path}/status", encoding="utf-8") as status:
        pid = re.search(r"^PID: (\d+)$", status.read(), re.MULTILINE)[1]
    executable = os.readlink(f"/proc/{pid}/exe")
    with open(f"/proc/{pid}/maps", encoding="utf-8") as maps:
        for line in maps:
            fields = line.split()
            if fields[2] == "00000000" and fields[-1] == executable:
                return fields[0].split("-")[0]
    raise RuntimeError(f"No mapping of {executable} in /proc/{pid}/maps")


bases = [executable_base(path) for path in sys.argv[1:]]
print(f"Executable base of the servers: {' '.join(bases)}")
if len(set(bases)) != 1:
    sys.exit("The servers have their executable at different addresses: both must be started with ASLR disabled")
