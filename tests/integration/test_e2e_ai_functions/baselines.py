"""Committed baselines for the structural and architecture metrics.

Only integers and dimensionless ratios belong here. Wall-clock milliseconds and
real-endpoint latencies are host- and endpoint-dependent, so they are compared run-local
instead (02-latency.md section 8).

Regenerate with `AI_E2E_WRITE_BASELINES=1` and review the diff; never hand-edit.
"""

import json
import os
import pathlib
import subprocess

BASELINE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "baselines")

# A baseline generated many commits ago may describe a different implementation. This is
# a warning threshold, not a failure: the suite is manual, so drift is possible.
STALENESS_COMMITS = 500


def _git(args, default=""):
    try:
        return subprocess.run(
            ["git"] + args,
            capture_output=True,
            text=True,
            timeout=30,
            cwd=os.path.dirname(os.path.realpath(__file__)),
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return default


def current_sha():
    """HEAD, read from git or - when git is absent, as in the runner image - from `.git`."""
    sha = _git(["rev-parse", "HEAD"])
    if sha:
        return sha
    root = pathlib.Path(__file__).resolve().parents[3] / ".git"
    try:
        head = (root / "HEAD").read_text().strip()
    except OSError:
        return ""
    if head.startswith("ref: "):
        ref = head[5:].strip()
        try:
            return (root / ref).read_text().strip()
        except OSError:
            try:
                for line in (root / "packed-refs").read_text().splitlines():
                    if line.endswith(f" {ref}"):
                        return line.split()[0]
            except OSError:
                return ""
        return ""
    return head


def _distance(sha):
    if not sha:
        return None
    output = _git(["rev-list", "--count", f"{sha}..HEAD"])
    return int(output) if output.isdigit() else None


def path_for(name):
    return os.path.join(BASELINE_DIR, f"{name}.json")


def load(name):
    try:
        with open(path_for(name)) as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def save(name, payload):
    os.makedirs(BASELINE_DIR, exist_ok=True)
    payload = dict(payload)
    payload["git_sha"] = current_sha()
    with open(path_for(name), "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    _give_back_ownership(BASELINE_DIR)
    _give_back_ownership(path_for(name))
    return path_for(name)


def _give_back_ownership(target):
    """Match the suite directory's owner.

    The integration job runs as root inside the container and writes here through a bind
    mount, so a freshly generated baseline would land root-owned on the host and the
    developer who has to review and commit it could not rewrite it. Best effort: on a
    setup where this is not permitted, the file is still written.
    """
    try:
        reference = os.stat(os.path.dirname(os.path.realpath(__file__)))
        os.chown(target, reference.st_uid, reference.st_gid)
    except (OSError, AttributeError):
        pass


def staleness_warning(baseline):
    """A human-readable warning when the baseline is far behind, else empty."""
    if not baseline:
        return ""
    distance = _distance(baseline.get("git_sha", ""))
    if distance is None:
        return (
            f"baseline was generated at {baseline.get('git_sha', '?')[:12]}, "
            "which is not in this history: treat its numbers with suspicion"
        )
    if distance > STALENESS_COMMITS:
        return (
            f"baseline is {distance} commits behind HEAD "
            f"(generated at {baseline.get('git_sha', '?')[:12]}): regenerate with "
            "AI_E2E_WRITE_BASELINES=1 and review the diff"
        )
    return ""
