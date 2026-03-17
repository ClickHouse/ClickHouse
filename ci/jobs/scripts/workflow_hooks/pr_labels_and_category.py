import re
import sys
from typing import Optional, Tuple

from praktika.info import Info
from praktika.utils import Shell

LABEL_CATEGORIES = {
    "pr-backward-incompatible": ["Backward Incompatible Change"],
    "pr-bugfix": [
        "Bug Fix",
        "Bug Fix (user-visible misbehavior in an official stable release)",
        "Bug Fix (user-visible misbehaviour in official stable or prestable release)",
        "Bug Fix (user-visible misbehavior in official stable or prestable release)",
    ],
    "pr-critical-bugfix": [
        "Critical Bug Fix (crash, data loss, RBAC) or LOGICAL_ERROR"
    ],
    "pr-build": [
        "Build/Testing/Packaging Improvement",
        "Build Improvement",
        "Build/Testing Improvement",
        "Build",
        "Packaging Improvement",
    ],
    "pr-documentation": [
        "Documentation (changelog entry is not required)",
        "Documentation",
    ],
    "pr-feature": ["New Feature"],
    "pr-improvement": ["Improvement"],
    "pr-not-for-changelog": [
        "Not for changelog (changelog entry is not required)",
        "Not for changelog",
    ],
    "pr-performance": ["Performance Improvement"],
    "pr-ci": [
        "CI Fix or Improvement (changelog entry is not required)",
        "CI Fix or Improvement",
    ],
    "pr-experimental": ["Experimental Feature"],
}

CATEGORY_TO_LABEL = {
    c: lb for lb, categories in LABEL_CATEGORIES.items() for c in categories
}

# Labels for categories that don't require a changelog entry
NO_CHANGELOG_REQUIRED_LABELS = {"pr-not-for-changelog", "pr-ci", "pr-documentation"}


class Labels:
    PR_BUGFIX = "pr-bugfix"
    PR_CRITICAL_BUGFIX = "pr-critical-bugfix"
    CAN_BE_TESTED = "can be tested"
    DO_NOT_TEST = "do not test"
    NO_FAST_TESTS = "no-fast-tests"
    CI_MACOS = "ci-macos"
    MUST_BACKPORT = "pr-must-backport"
    JEPSEN_TEST = "jepsen-test"
    SKIP_MERGEABLE_CHECK = "skip mergeable check"
    PR_BACKPORT = "pr-backport"
    PR_BACKPORTS_CREATED = "pr-backports-created"
    PR_BACKPORTS_CREATED_CLOUD = "pr-backports-created-cloud"
    PR_CHERRYPICK = "pr-cherrypick"
    PR_CI = "pr-ci"
    PR_FEATURE = "pr-feature"
    PR_EXPERIMENTAL = "pr-experimental"
    PR_PERFORMANCE = "pr-performance"
    PR_SYNCED_TO_CLOUD = "pr-synced-to-cloud"
    PR_SYNC_UPSTREAM = "pr-sync-upstream"
    RELEASE = "release"
    RELEASE_LTS = "release-lts"
    SUBMODULE_CHANGED = "submodule changed"

    CI_BUILD = "ci-build"

    CI_PERFORMANCE = "ci-performance"

    CI_INTEGRATION_FLAKY = "ci-integration-test-flaky"
    CI_INTEGRATION = "ci-integration-test"

    CI_FUNCTIONAL_FLAKY = "ci-functional-test-flaky"
    CI_FUNCTIONAL = "ci-functional-test"
    CI_TOOLCHAIN = "ci-toolchain"

    # automatic backport for critical bug fixes
    AUTO_BACKPORT = {"pr-critical-bugfix"}


def _levenshtein(s1: str, s2: str) -> int:
    """Compute Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein(s2, s1)
    if not s2:
        return len(s1)
    prev = list(range(len(s2) + 1))
    for c1 in s1:
        curr = [prev[0] + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (c1 != c2)))
        prev = curr
    return prev[-1]


def _normalize_for_matching(cat: str) -> str:
    """Normalize category for matching: strip parenthetical suffix, collapse whitespace, casefold."""
    pos = cat.find("(")
    result = cat[:pos] if pos != -1 else cat
    return re.sub(r"\s+", " ", result.strip()).casefold()


def _normalized_distance(s1: str, s2: str) -> float:
    """Compute normalized Levenshtein distance (0.0 = identical, 1.0 = completely different)."""
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 0.0
    return _levenshtein(s1, s2) / max_len


# Maximum normalized Levenshtein distance for fuzzy matching (20%)
MAX_NORMALIZED_DISTANCE = 0.2


def find_category(category: str) -> Tuple[Optional[str], Optional[str]]:
    """Find matching category and label using fuzzy matching.

    Matching is case-insensitive, whitespace-insensitive, ignores parenthetical
    suffixes, and allows normalized Levenshtein distance up to 20%
    (with whitespace removed for comparison).

    Returns (matched_category, label) or (None, None).
    """
    # Exact match
    if category in CATEGORY_TO_LABEL:
        return category, CATEGORY_TO_LABEL[category]

    # Normalized exact match
    norm = _normalize_for_matching(category)
    for known_cat, label in CATEGORY_TO_LABEL.items():
        if _normalize_for_matching(known_cat) == norm:
            return known_cat, label

    # Fuzzy match with normalized Levenshtein distance <= 20% (whitespace removed)
    compact = norm.replace(" ", "")
    best_dist = MAX_NORMALIZED_DISTANCE + 1e-9
    best_match = None
    seen = set()
    for known_cat, label in CATEGORY_TO_LABEL.items():
        known_compact = _normalize_for_matching(known_cat).replace(" ", "")
        if known_compact in seen:
            continue
        seen.add(known_compact)
        dist = _normalized_distance(compact, known_compact)
        if dist < best_dist:
            best_dist = dist
            best_match = (known_cat, label)

    return best_match if best_match else (None, None)


def get_category(pr_body: str) -> Tuple[str, str]:
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    category = ""
    error = ""
    i = 0
    while i < len(lines):
        m = re.match(
            r"(?i)^[#>*_ ]*change\s*log\s*category(?:[^:]*:\s*(.*))?$", lines[i]
        )
        if m:
            # Check if the category is on the same line (e.g. "Changelog category: Bug Fix")
            inline = (m.group(1) or "").strip()
            inline = re.sub(r"^[-*_\s]*", "", inline)
            inline = re.sub(r"[-*_\s]*$", "", inline)
            if inline:
                new_category = inline
                i += 1
            else:
                i += 1
                if i >= len(lines):
                    break
                # Can have one empty line between header and the category
                # itself. Filter it out.
                if not lines[i]:
                    i += 1
                    if i >= len(lines):
                        break
                new_category = re.sub(r"^[-*\s]*", "", lines[i])
                i += 1

                # Should not have more than one category. Require empty line
                # after the first found category.
                if i < len(lines) and lines[i]:
                    second_category = re.sub(r"^[-*\s]*", "", lines[i])
                    error = f"More than one changelog category specified: '{new_category}', '{second_category}'"
                    break

            if category:
                error = f"More than one changelog category specified: '{category}', '{new_category}'"
                break
            category = new_category
        else:
            i += 1

    if not category:
        if "Reverts ClickHouse/" in pr_body:
            return "", LABEL_CATEGORIES["pr-not-for-changelog"][0]
        return "Change category is missing or invalid", ""

    matched, _label = find_category(category)
    if matched is None:
        if "Reverts ClickHouse/" in pr_body:
            return "", LABEL_CATEGORIES["pr-not-for-changelog"][0]
        return f"Change category is missing or invalid: '{category}'", ""

    return error, matched


def check_labels(category, info):
    pr_labels_to_add = []
    pr_labels_to_remove = []
    labels = info.pr_labels
    _matched, label_for_category = find_category(category)
    if label_for_category and label_for_category not in labels:
        pr_labels_to_add.append(label_for_category)

    # Labels that should not be auto-removed even if they don't match the current
    # category, because they serve additional purposes (e.g., enabling performance
    # tests in CI when set manually).
    protected_labels = {Labels.PR_PERFORMANCE}

    for label in labels:
        if (
            label in CATEGORY_TO_LABEL.values()
            and label_for_category
            and label != label_for_category
            and label not in protected_labels
        ):
            pr_labels_to_remove.append(label)

    if info.pr_number:
        changed_files = info.get_kv_data("changed_files")
        if "contrib/" in " ".join(changed_files):
            pr_labels_to_add.append(Labels.SUBMODULE_CHANGED)

    if any(label in Labels.AUTO_BACKPORT for label in pr_labels_to_add):
        backport_labels = [Labels.MUST_BACKPORT]
        pr_labels_to_add += [label for label in backport_labels if label not in labels]
        print(f"Add backport labels [{backport_labels}] for PR category [{category}]")

    cmd = f"gh pr edit {info.pr_number}"
    if pr_labels_to_add:
        print(f"Add labels [{pr_labels_to_add}]")
        for label in pr_labels_to_add:
            cmd += f" --add-label '{label}'"
            if label in info.pr_labels:
                info.pr_labels.append(label)
            info.dump()

    if pr_labels_to_remove:
        print(f"Remove labels [{pr_labels_to_remove}]")
        for label in pr_labels_to_remove:
            cmd += f" --remove-label '{label}'"
            if label in info.pr_labels:
                info.pr_labels.remove(label)
            info.dump()

    if pr_labels_to_remove or pr_labels_to_add:
        Shell.check(cmd, verbose=True, strict=True, retries=5)


if __name__ == "__main__":
    info = Info()
    if Labels.RELEASE in info.pr_labels or Labels.RELEASE_LTS in info.pr_labels:
        print("NOTE: Release PR detected, skipping changelog category check")
        sys.exit(0)
    error, category = get_category(info.pr_body)
    if not category or error:
        print(f"ERROR: {error}")
        sys.exit(1)
    check_labels(category, info)
