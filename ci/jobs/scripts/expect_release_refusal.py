#!/usr/bin/env python3
"""Run release_job.py and pass a dry-run negative check on a no-candidate skip or an expected prepare() refusal with clean follow-up; fail on anything else."""
import json
import subprocess
import sys

from ci.praktika._environment import _Environment
from ci.praktika.result import Result

_REFUSAL_STEP = "Prepare Release Info"


def _score_expected_refusal(expected: str) -> int:
    path = Result.file_name_static(_Environment.get().JOB_NAME)
    with open(path, "r", encoding="utf-8") as f:
        result = json.load(f)

    # Only the Prepare Release Info leaf is expected to fail (the refusal); any other failed leaf is a real failure that must keep the job red.
    other_failures = []

    def visit(node):
        children = node.get("results") or []
        if not children and node.get("status") not in (
            "OK",
            "XFAIL",
            "XPASS",
            "SKIPPED",
        ):
            if node.get("name") == _REFUSAL_STEP:
                node["status"] = "XFAIL"
            else:
                other_failures.append(node.get("name"))
        for child in children:
            visit(child)

    visit(result)
    if other_failures:
        print(f"ERROR: release refused as expected, but other steps failed: {other_failures}")
        return 1

    # The job passed (it refused as required); Mergeable Check blocks any non-OK top-level status, so mark it OK, not XFAIL.
    result["status"] = "OK"
    result["info"] = f"Refused as expected with [{expected}]"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4)
    return 0


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
        # No candidate to rehearse is a pass; any other clean exit means the guard never fired.
        if "No commit to" in proc.stdout and "skipping" in proc.stdout:
            print("No candidate in the live release state; nothing to rehearse")
            return 0
        print(f"ERROR: expected the release job to be refused with [{expected}], but it succeeded")
        return 1
    if expected not in proc.stdout:
        print(f"ERROR: the release job was refused, but not with the expected message [{expected}]")
        return 1
    print(f"Refused as expected with [{expected}]")
    return _score_expected_refusal(expected)


if __name__ == "__main__":
    sys.exit(main())
