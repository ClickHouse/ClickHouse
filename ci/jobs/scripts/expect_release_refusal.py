#!/usr/bin/env python3
"""Run release_job.py and pass only if it is refused with the expected message.

The out-of-order and recovery-misuse dry-run PR checks assert that prepare()
*rejects* an invalid dispatch, so here a zero exit (the run was not refused) is
the failure, and the refusal message must match so an unrelated failure is not
scored as a pass."""
import subprocess
import sys


def main() -> int:
    assert "--expect" in sys.argv and "--" in sys.argv, (
        "usage: expect_release_refusal.py --expect <substring> -- <release_job.py args>"
    )
    i = sys.argv.index("--expect")
    expected = sys.argv[i + 1]
    job_args = sys.argv[sys.argv.index("--", i + 2) + 1 :]
    proc = subprocess.run(
        [sys.executable, "./ci/jobs/release_job.py", *job_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(proc.stdout, end="")
    if proc.returncode == 0:
        print(f"ERROR: expected the release job to be refused with [{expected}], but it succeeded")
        return 1
    if expected not in proc.stdout:
        print(f"ERROR: the release job was refused, but not with the expected message [{expected}]")
        return 1
    print(f"Refused as expected with [{expected}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
