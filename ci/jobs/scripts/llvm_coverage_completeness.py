"""Completeness metadata for the LLVM coverage diff gate.

The gate compares a baseline coverage percentage against the current one. Both
percentages are merged from per-shard `.profdata` artifacts that are declared
`optional=True`, so either side can silently be short a shard and still produce
a number. This module carries the fact "the measurement was complete" next to
the measurement so the consumer can refuse to compare two things that are not
comparable.

`manifest_fp` fingerprints the ARTIFACT MANIFEST only. It detects a change to
the set of coverage artifacts (the list has really grown 18 -> 20 -> 21), which
makes two otherwise-complete measurements incomparable. It deliberately does
NOT claim that the two runs exercised the same code: coverage test selection
also depends on the test tree, on per-test tags and on hash-based sharding, none
of which is expressible here. See the PR description for that residual.

Consumer-only by design: the producers derive their profile filename inline
from their own artifact identity, so no producer cache digest depends on this
file.
"""

import hashlib
import json
import os
from typing import Iterable, List, Optional, Tuple

# Bump when the on-disk shape changes. A reader that meets a higher version must
# treat the sidecar as unknown rather than misinterpret it.
SCHEMA_VERSION = 1

SIDECAR_BASENAME = "llvm_coverage.meta.json"

# Shard profiles are named "<artifact name>.profdata" by their producer, so the
# filename carries the shard identity and completeness is exact set equality.
PROFDATA_SUFFIX = ".profdata"


def profile_basename(artifact_name: str) -> str:
    """Profile filename for one coverage artifact.

    Both sides must derive the name the same way: the producers call this shape
    inline (they must not import this module, so their cache digests stay
    independent of it) and the consumer builds its expected set from it.
    """
    assert isinstance(artifact_name, str) and artifact_name, (
        f"coverage artifact name must be a non-empty str, got {artifact_name!r}"
    )
    return f"{artifact_name}{PROFDATA_SUFFIX}"


def manifest_fingerprint(artifact_names: Iterable[str]) -> str:
    """Digest of the sorted artifact-name list.

    Detects manifest drift only. Two runs with different fingerprints measured
    different artifact sets and must not be compared.
    """
    names = sorted(str(n) for n in artifact_names)
    payload = "\n".join(names).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def file_digest(path: str) -> str:
    """Content digest of a coverage `.info`, or "" when it does not exist.

    Uploads of the `.info` and of this sidecar are separate S3 objects written
    sequentially into a commit-keyed prefix, so a re-run of the same commit can
    replace one before the other. Recording the digest of the `.info` the
    sidecar describes lets the consumer detect such a torn pair.
    """
    if not path or not os.path.exists(path):
        return ""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def present_profiles(directory: str) -> List[str]:
    """Profile filenames present in `directory`.

    Snapshot this BEFORE the aggregate merge runs: the merge writes its own
    output into the same directory and would otherwise appear as an extra input.
    """
    if not os.path.isdir(directory):
        return []
    return sorted(
        name
        for name in os.listdir(directory)
        if name.endswith(PROFDATA_SUFFIX)
        and os.path.isfile(os.path.join(directory, name))
    )


def classify(expected_names: Iterable[str], present_files: Iterable[str]):
    """Compare the expected profile set against what is on disk.

    Returns (complete, missing, unexpected).

    Completeness is exact set EQUALITY, never a difference: `expected - present`
    is empty whenever every expected file is there, so it silently accepts a
    foreign or stale profile that the merge would then fold into the total.
    """
    expected = {profile_basename(n) for n in expected_names}
    present = set(present_files)
    missing = sorted(expected - present)
    unexpected = sorted(present - expected)
    return (not missing and not unexpected), missing, unexpected


def merge_inputs(expected_names: Iterable[str], present_files: Iterable[str]) -> List[str]:
    """Files to hand the aggregate merge: `expected` INTERSECT `present`.

    Two different sets are in play and confusing them turns an incomplete run
    into a hard failure: the merge gets the shards that actually arrived (so a
    legitimately absent optional shard still merges the rest), while the
    completeness VERDICT is the equality above.
    """
    expected = {profile_basename(n) for n in expected_names}
    return sorted(expected & set(present_files))


def build_sidecar(
    artifact_names: Iterable[str],
    present_files: Iterable[str],
    info_path: str = "",
    merge_ok: bool = True,
) -> dict:
    names = sorted(str(n) for n in artifact_names)
    complete, missing, unexpected = classify(names, present_files)
    return {
        "schema_version": SCHEMA_VERSION,
        "manifest_fp": manifest_fingerprint(names),
        "complete": bool(complete and merge_ok),
        "merge_ok": bool(merge_ok),
        "expected": [profile_basename(n) for n in names],
        "present": sorted(present_files),
        "missing": missing,
        "unexpected": unexpected,
        "info_digest": file_digest(info_path),
    }


def write_sidecar(path: str, sidecar: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sidecar, f, indent=2, sort_keys=True)


def read_sidecar(path: str) -> Optional[dict]:
    """Read a sidecar, or None when it is absent or unusable.

    None means "unknown", which the consumer turns into SKIPPED. No master
    commit published before this change has a sidecar, so absence must never be
    a hard failure or every PR would be blocked until master republishes.
    """
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (ValueError, OSError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def current_side_reason(current: dict) -> str:
    """Why the CURRENT side alone cannot support a verdict, "" when it can.

    These causes depend only on the current sidecar, so they are knowable before
    the differential script runs. `comparable` delegates to this function, which
    keeps a single producer of these strings: a caller short-circuiting on a
    current-side cause cannot drift from the verdict `comparable` would give.
    """
    if not isinstance(current, dict):
        return "current-side completeness metadata is missing"

    if current.get("schema_version") != SCHEMA_VERSION:
        return (
            f"current-side sidecar schema version {current.get('schema_version')!r}"
            f" is not {SCHEMA_VERSION}"
        )

    if not current.get("merge_ok", True):
        return "the aggregate coverage merge failed, so this run has no complete measurement"

    if not current.get("complete"):
        missing = current.get("missing") or []
        unexpected = current.get("unexpected") or []
        detail = []
        if missing:
            detail.append(f"missing {len(missing)}: {', '.join(missing)}")
        if unexpected:
            detail.append(f"unexpected {len(unexpected)}: {', '.join(unexpected)}")
        return "PR-side measurement is incomplete (" + "; ".join(detail) + ")"

    return ""


def comparable(
    current: dict,
    baseline: Optional[dict],
    baseline_info_path: str = "",
) -> Tuple[bool, str]:
    """Whether a verdict may be produced from these two measurements.

    Returns (comparable, reason). `reason` is empty when comparable and
    otherwise names which side was short and what was missing, so the SKIPPED
    sub-result explains itself.
    """
    current_reason = current_side_reason(current)
    if current_reason:
        return False, current_reason

    if baseline is None:
        return False, (
            "baseline commit published no completeness metadata, so its measurement"
            " cannot be confirmed complete"
        )

    if baseline.get("schema_version") != SCHEMA_VERSION:
        return False, (
            f"baseline sidecar schema version {baseline.get('schema_version')!r}"
            f" is not {SCHEMA_VERSION}"
        )

    if not baseline.get("complete"):
        missing = baseline.get("missing") or []
        detail = f"missing {len(missing)}: {', '.join(missing)}" if missing else "reason unrecorded"
        return False, f"baseline measurement is incomplete ({detail})"

    if baseline.get("manifest_fp") != current.get("manifest_fp"):
        return False, (
            "coverage artifact manifest changed between the baseline and this run"
            f" (baseline {baseline.get('manifest_fp')!r} vs current {current.get('manifest_fp')!r}),"
            " so the two totals cover different artifact sets"
        )

    recorded = baseline.get("info_digest") or ""
    if recorded and baseline_info_path:
        actual = file_digest(baseline_info_path)
        if actual and actual != recorded:
            return False, (
                "baseline sidecar does not describe the baseline coverage data it was"
                f" fetched with (recorded {recorded!r} vs actual {actual!r})"
            )

    return True, ""
