import re
import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import (
    CATEGORIES_FOLD,
    LABEL_CATEGORIES,
    get_category,
    normalize_category,
)
from ci.praktika.gh import GH
from ci.praktika.info import Info


def check_changelog_entry(category, pr_body: str) -> str:
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    if category in LABEL_CATEGORIES["pr-not-for-changelog"]:
        return ""
    if category in LABEL_CATEGORIES["pr-ci"]:
        return ""
    if category in LABEL_CATEGORIES["pr-documentation"]:
        return ""
    if normalize_category(category) not in CATEGORIES_FOLD:
        return f"Invalid category: [{category}]"

    entry = ""
    i = 0
    while i < len(lines):
        if re.match(
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

    error = ""
    if not entry:
        error = f"Changelog entry required for category '{category}'"
    return error


if __name__ == "__main__":

    title, body, labels = GH.get_pr_title_body_labels()
    if not title or not body:
        print("WARNING: Failed to get PR title or body, read from environment")

    body = Info().pr_body
    error, category = get_category(body)
    if error or not category:
        print(f"ERROR: {error}")
        sys.exit(1)

    error = check_changelog_entry(category, body)
    if error:
        print(f"ERROR: {error}")
        sys.exit(1)
