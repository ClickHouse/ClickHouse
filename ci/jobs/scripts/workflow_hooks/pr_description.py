import re
import sys
from typing import Tuple

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
    "pr-ci": ["CI Fix or Improvement (changelog entry is not required)"],
    "pr-experimental": ["Experimental Feature"],
}

CATEGORY_TO_LABEL = {
    c: lb for lb, categories in LABEL_CATEGORIES.items() for c in categories
}


class Labels:
    PR_BUGFIX = "pr-bugfix"
    PR_CRITICAL_BUGFIX = "pr-critical-bugfix"
    CAN_BE_TESTED = "can be tested"
    DO_NOT_TEST = "do not test"
    NO_FAST_TESTS = "no-fast-tests"
    MUST_BACKPORT = "pr-must-backport"
    MUST_BACKPORT_CLOUD = "pr-must-backport-cloud"
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

    CI_PERFORMANCE = "ci-performance"

    # automatic backport for critical bug fixes
    AUTO_BACKPORT = {"pr-critical-bugfix"}


def normalize_category(cat: str) -> str:
    """Drop everything after open parenthesis, drop leading/trailing whitespaces, normalize case"""
    pos = cat.find("(")

    result = cat[:pos] if pos != -1 else cat
    return result.strip().casefold()


CATEGORIES_FOLD = [
    normalize_category(c)
    for lb, categories in LABEL_CATEGORIES.items()
    for c in categories
]


def check_category(pr_body: str) -> Tuple[bool, str]:
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    if "Reverts ClickHouse/" in pr_body:
        return True, LABEL_CATEGORIES["pr-not-for-changelog"][0]

    category = ""
    entry = ""
    description_error = ""

    i = 0
    while i < len(lines):
        if re.match(r"(?i)^[#>*_ ]*change\s*log\s*category", lines[i]):
            i += 1
            if i >= len(lines):
                break
            # Can have one empty line between header and the category
            # itself. Filter it out.
            if not lines[i]:
                i += 1
                if i >= len(lines):
                    break
            category = re.sub(r"^[-*\s]*", "", lines[i])
            i += 1

            # Should not have more than one category. Require empty line
            # after the first found category.
            if i >= len(lines):
                break
            if lines[i]:
                second_category = re.sub(r"^[-*\s]*", "", lines[i])
                description_error = (
                    "More than one changelog category specified: "
                    f"'{category}', '{second_category}'"
                )
                break

        elif re.match(
            r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
        ):
            i += 1
            # Can have one empty line between header and the entry itself.
            # Filter it out.
            if i < len(lines) and not lines[i]:
                i += 1
            # All following lines until empty one are the changelog entry.
            entry_lines = []
            while i < len(lines) and lines[i]:
                entry_lines.append(lines[i])
                i += 1
            entry = " ".join(entry_lines)
            # Don't accept changelog entries like '...'.
            entry = re.sub(r"[#>*_.\- ]", "", entry)
            # Don't accept changelog entries like 'Close #12345'.
            entry = re.sub(r"^[\w\-\s]{0,10}#?\d{5,6}\.?$", "", entry)
        else:
            i += 1

    if not description_error:
        if not category:
            description_error = "Changelog category is empty"
        elif normalize_category(category) not in CATEGORIES_FOLD:
            description_error = f"Category '{category}' is not valid"
        elif not entry and "(changelog entry is not required)" not in category:
            description_error = f"Changelog entry required for category '{category}'"

    print(description_error)
    return not description_error, category


def check_labels(category, info):
    pr_labels_to_add = []
    pr_labels_to_remove = []
    labels = info.pr_labels
    if category in CATEGORY_TO_LABEL and CATEGORY_TO_LABEL[category] not in labels:
        pr_labels_to_add.append(CATEGORY_TO_LABEL[category])

    for label in labels:
        if (
            label in CATEGORY_TO_LABEL.values()
            and category in CATEGORY_TO_LABEL
            and label != CATEGORY_TO_LABEL[category]
        ):
            pr_labels_to_remove.append(label)

    if info.pr_number:
        changed_files = info.get_custom_data("changed_files")
        if "contrib/" in " ".join(changed_files):
            pr_labels_to_add.append(Labels.SUBMODULE_CHANGED)

    if any(label in Labels.AUTO_BACKPORT for label in pr_labels_to_add):
        backport_labels = [Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_CLOUD]
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
        Shell.check(cmd, verbose=True, strict=True)


if __name__ == "__main__":
    info = Info()
    is_ok, category = check_category(info.pr_body)
    if not is_ok:
        sys.exit(1)
    check_labels(category, info)
