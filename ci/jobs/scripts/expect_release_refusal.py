#!/usr/bin/env python3
"""Run release_job.py and pass only if it is refused with the expected message.

The out-of-order and recovery-misuse dry-run PR checks assert that prepare()
*rejects* an invalid dispatch, so here a zero exit (the run was not refused) is
the failure, and the refusal message must match so an unrelated failure is not
scored as a pass."""
import json
import subprocess
import sys

from ci.praktika._environment import _Environment
from ci.praktika.result import Result


def _mark_refusal_expected(expected: str) -> None:
    # The child release_job.py ran under this job's JOB_NAME and, on the refusal
    # we expect, wrote a FAIL result file that the outer runner reads as the job
    # verdict. Rewrite that file so the expected refusal reads as XFAIL (scored
    # OK) instead of a failure.
    path = Result.file_name_static(_Environment.get().JOB_NAME)

    def xfail(node):
        if node.get("status") not in ("OK", "XFAIL", "XPASS", "SKIPPED"):
            node["status"] = "XFAIL"
        for child in node.get("results") or []:
            xfail(child)

    with open(path, "r", encoding="utf-8") as f:
        result = json.load(f)
    xfail(result)
    result["info"] = f"Refused as expected with [{expected}]"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)


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
    _mark_refusal_expected(expected)
    return 0


if __name__ == "__main__":
    sys.exit(main())
