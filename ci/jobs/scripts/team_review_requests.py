import json
import os
import re
import sys
from pathlib import Path

from ci.praktika.gh import GH

DOCS_PREFIX = "docs/"
SOURCE_PREFIX = "src/"
CLICKPIPES_DOCS_PREFIX = "docs/integrations/clickpipes/"
INTEGRATIONS_DOCS_PREFIXES = (
    "docs/integrations/language-clients/",
    "docs/integrations/connectors/",
)

DOCS_TEAM = "docs"
CLICKPIPES_TEAM = "clickpipes"
INTEGRATIONS_ECOSYSTEM_TEAM = "integrations-ecosystem"

CAN_BE_TESTED = "can be tested"
REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def normalize_path(file):
    return file.removeprefix(".").removeprefix("/")


def get_docs_teams_to_request(changed_files):
    files = [normalize_path(file) for file in changed_files]
    if any(file.startswith(SOURCE_PREFIX) for file in files):
        return []

    docs_files = [file for file in files if file.startswith(DOCS_PREFIX)]
    if not docs_files:
        return []

    teams = []
    if any(file.startswith(CLICKPIPES_DOCS_PREFIX) for file in docs_files):
        teams.append(CLICKPIPES_TEAM)

    if any(
        file.startswith(prefix)
        for file in docs_files
        for prefix in INTEGRATIONS_DOCS_PREFIXES
    ):
        teams.append(INTEGRATIONS_ECOSYSTEM_TEAM)

    teams.append(DOCS_TEAM)
    return teams


def is_authorized_event(event):
    repository = event["repository"]["full_name"]
    pull_request = event["pull_request"]
    if pull_request["base"]["repo"]["full_name"] != repository:
        raise RuntimeError(
            "Pull request base repository does not match the event repository"
        )

    if pull_request["head"]["repo"]["full_name"] == repository:
        return True

    labels = {label["name"] for label in pull_request.get("labels", [])}
    return CAN_BE_TESTED in labels


def get_changed_files(repository, pull_request_number):
    if not REPOSITORY_PATTERN.fullmatch(repository):
        raise ValueError(f"Invalid repository name [{repository}]")
    if not isinstance(pull_request_number, int) or pull_request_number <= 0:
        raise ValueError(f"Invalid pull request number [{pull_request_number}]")

    command = (
        f"gh api repos/{repository}/pulls/{pull_request_number}/files "
        "--paginate --jq '.[] | .filename, (.previous_filename // empty)'"
    )
    output = GH.get_output_with_retries(command, strict=True)
    return list(dict.fromkeys(output.splitlines())) if output else []


def check(event):
    if not is_authorized_event(event):
        print(
            f"Skip team review requests: fork PR lacks the [{CAN_BE_TESTED}] label"
        )
        return True

    repository = event["repository"]["full_name"]
    pull_request_number = event["pull_request"]["number"]
    changed_files = get_changed_files(repository, pull_request_number)
    return GH.request_team_reviews(
        get_docs_teams_to_request(changed_files),
        pr=pull_request_number,
        repo=repository,
    )


def main():
    event_path = os.environ.get("GITHUB_EVENT_PATH", "")
    if not event_path:
        raise RuntimeError("GITHUB_EVENT_PATH is not set")
    with Path(event_path).open(encoding="utf-8") as event_file:
        event = json.load(event_file)
    if not check(event):
        sys.exit(1)


if __name__ == "__main__":
    main()
