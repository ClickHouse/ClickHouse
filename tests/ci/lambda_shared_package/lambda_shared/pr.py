#!/usr/bin/env python

import re
from typing import Tuple

# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "achimbab",
        "adevyatova ",  # DOCSUP
        "Algunenano",  # Raúl Marín, Tinybird
        "amosbird",
        "AnaUvarova",  # DOCSUP
        "anauvarova",  # technical writer, Yandex
        "annvsh",  # technical writer, Yandex
        "atereh",  # DOCSUP
        "azat",
        "bharatnc",  # Newbie, but already with many contributions.
        "bobrik",  # Seasoned contributor, CloudFlare
        "BohuTANG",
        "codyrobert",  # Flickerbox engineer
        "cwurm",  # Employee
        "damozhaeva",  # DOCSUP
        "den-crane",
        "flickerbox-tom",  # Flickerbox
        "gyuton",  # DOCSUP
        "hagen1778",  # Roman Khavronenko, seasoned contributor
        "hczhcz",
        "hexiaoting",  # Seasoned contributor
        "ildus",  # adjust, ex-pgpro
        "javisantana",  # a Spanish ClickHouse enthusiast, ex-Carto
        "ka1bi4",  # DOCSUP
        "kirillikoff",  # DOCSUP
        "kreuzerkrieg",
        "lehasm",  # DOCSUP
        "michon470",  # DOCSUP
        "nikvas0",
        "nvartolomei",
        "olgarev",  # DOCSUP
        "otrazhenia",  # Yandex docs contractor
        "pdv-ru",  # DOCSUP
        "podshumok",  # cmake expert from QRator Labs
        "s-mx",  # Maxim Sabyanin, former employee, present contributor
        "sevirov",  # technical writer, Yandex
        "spongedu",  # Seasoned contributor
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "vdimir",  # Employee
        "vzakaznikov",
        "YiuRULE",
        "zlobober",  # Developer of YT
        "ilejn",  # Arenadata, responsible for Kerberized Kafka
        "thomoco",  # ClickHouse
        "BoloniniD",  # Seasoned contributor, HSE
        "tonickkozlov",  # Cloudflare
        "tylerhannan",  # ClickHouse Employee
        "myrrc",  # Mike Kot, DoubleCloud
        "thevar1able",  # ClickHouse Employee
        "aalexfvk",
        "MikhailBurdukov",
        "tsolodov",  # ClickHouse Employee
        "kitaisreal",
    ]
}

# Descriptions are used in .github/PULL_REQUEST_TEMPLATE.md, keep comments there
# updated accordingly
# The following lists are append only, try to avoid editing them
# They still could be cleaned out after the decent time though.
LABELS = {
    "pr-backward-incompatible": ["Backward Incompatible Change"],
    "pr-bugfix": [
        "Bug Fix",
        "Bug Fix (user-visible misbehavior in an official stable release)",
        "Bug Fix (user-visible misbehaviour in official stable or prestable release)",
        "Bug Fix (user-visible misbehavior in official stable or prestable release)",
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
}

CATEGORY_TO_LABEL = {c: lb for lb, categories in LABELS.items() for c in categories}


def check_pr_description(pr_body: str, repo_name: str) -> Tuple[str, str]:
    """The function checks the body to being properly formatted according to
    .github/PULL_REQUEST_TEMPLATE.md, if the first returned string is not empty,
    then there is an error."""
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    # Check if body contains "Reverts ClickHouse/ClickHouse#36337"
    if [True for line in lines if re.match(rf"\AReverts {repo_name}#[\d]+\Z", line)]:
        return "", LABELS["pr-not-for-changelog"][0]

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
                return description_error, category

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

    if not category:
        description_error = "Changelog category is empty"
    # Filter out the PR categories that are not for changelog.
    elif re.match(
        r"(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)",
        category,
    ):
        pass  # to not check the rest of the conditions
    elif category not in CATEGORY_TO_LABEL:
        description_error, category = f"Category '{category}' is not valid", ""
    elif not entry:
        description_error = f"Changelog entry required for category '{category}'"

    return description_error, category
