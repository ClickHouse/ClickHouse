import re
import sys

from praktika.info import Info
from praktika.utils import Shell

from ci.jobs.scripts.workflow_hooks.pr_description import (
    CATEGORIES_FOLD,
    CATEGORY_TO_LABEL,
    Labels,
    normalize_category,
)


def get_category(pr_body: str) -> str:
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    category = ""
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
            break
        else:
            i += 1
    if not category or normalize_category(category) not in CATEGORIES_FOLD:
        return ""
    return category


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
        changed_files = info.get_kv_data("changed_files")
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
    category = get_category(info.pr_body)
    if not category:
        sys.exit(1)
    check_labels(category, info)
