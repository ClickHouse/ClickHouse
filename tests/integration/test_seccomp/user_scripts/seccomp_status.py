#!/usr/bin/python3

# Reports what a process the server started sees of the policy, as `<mode>\t<errno>`:
#  - the seccomp mode it inherited across `fork` and `execve`: 0 is no filter, 2 is a BPF filter;
#  - what `getxattr`, which the policy does not allow, does here. Without a filter the attribute is
#    simply missing (`ENODATA`); under the `errno` mode the call is refused (`EPERM`).

import errno
import os

if __name__ == "__main__":
    mode = "unknown"
    with open("/proc/self/status", "r") as status:
        for line in status:
            if line.startswith("Seccomp:"):
                mode = line.split(":", 1)[1].strip()

    try:
        os.getxattr("/etc/hostname", "user.nonexistent")
        getxattr_result = "0"
    except OSError as e:
        getxattr_result = errno.errorcode.get(e.errno, str(e.errno))

    print(mode + "\t" + getxattr_result)
