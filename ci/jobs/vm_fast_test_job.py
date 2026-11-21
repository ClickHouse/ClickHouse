"""Run the macOS fast test inside a throwaway Tart VM on the runner host.

The VM is cloned from the provisioned image (`macos_vm.ensure_golden`, built once per host and tool
set), gets the checkout at the same absolute path so the result file's artifact paths stay valid,
runs `fast_test_darwin.sh`, returns `ci/tmp` to the host for praktika to report, and is deleted.
"""

import os
import shlex
import sys
import time
from pathlib import Path

from ci.jobs.scripts import macos_vm

REPO = Path.cwd()
# Host-specific variables; everything else (praktika's job context, secrets) is forwarded.
NOT_FORWARDED = {"HOME", "USER", "LOGNAME", "SHELL", "PATH", "PWD", "OLDPWD", "TMPDIR", "TART_HOME"}


def guest_env():
    lines = [
        f"export {k}={shlex.quote(v)}"
        for k, v in os.environ.items()
        if k not in NOT_FORWARDED and not k.startswith(("SSH_", "XPC_", "__CF")) and k.isidentifier()
    ]
    return "\n".join(lines + [f"export PATH={shlex.quote(macos_vm.GUEST_PATH)}"])


def main():
    golden = macos_vm.ensure_golden()
    with macos_vm.VM.clone(golden, f"job-ft-{int(time.time())}") as vm:
        vm.start()
        vm.ssh(f"sudo mkdir -p {shlex.quote(str(REPO))} && sudo chown {macos_vm.GUEST_USER} {shlex.quote(str(REPO))}")
        macos_vm.log("copying the checkout and job inputs into the VM")
        vm.rsync(f"{REPO}/", str(REPO), excludes=["ci/tmp/run*"])
        rc = vm.ssh(f"set -e\n{guest_env()}\ncd {shlex.quote(str(REPO))}\nset +e\n./ci/jobs/scripts/fast_test_darwin.sh\n", check=False)
        macos_vm.log(f"fast test in the VM exited with {rc}; copying ci/tmp back")
        vm.rsync(f"{REPO}/ci/tmp/", f"{REPO}/ci/tmp", to_guest=False, excludes=["run*"])
    return rc


if __name__ == "__main__":
    sys.exit(main())
