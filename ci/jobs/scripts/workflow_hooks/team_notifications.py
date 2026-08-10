import sys

from ci.praktika.gh import GH
from ci.praktika.info import Info

INTEGRATIONS_ECOSYSTEM_FILES = ("src/Core/TypeId.h",)

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


def normalize_path(file):
    return file.removeprefix(".").removeprefix("/")


def get_docs_teams_to_request(changed_files):
    files = [normalize_path(file) for file in changed_files]
    if any(file.startswith(SOURCE_PREFIX) for file in files):
        return []

    files = [file for file in files if file.startswith(DOCS_PREFIX)]
    teams = []

    if not files:
        return teams

    if any(file.startswith(CLICKPIPES_DOCS_PREFIX) for file in files):
        teams.append(CLICKPIPES_TEAM)

    if any(
        file.startswith(prefix)
        for file in files
        for prefix in INTEGRATIONS_DOCS_PREFIXES
    ):
        teams.append(INTEGRATIONS_ECOSYSTEM_TEAM)

    teams.append(DOCS_TEAM)
    return teams


def check():
    info = Info()

    changed_files = info.get_kv_data("changed_files")
    assert changed_files is not None, (
        "changed_files is not populated in JOB_KV_DATA: the store_data pre-hook "
        "most likely failed to fetch the PR file list from the GitHub API. "
        "See the Config Workflow logs for the underlying error."
    )
    if info.event_action == "opened":
        GH.request_team_reviews(get_docs_teams_to_request(changed_files))

    if any(
        file.startswith(prefix)
        for file in changed_files
        for prefix in INTEGRATIONS_ECOSYSTEM_FILES
    ):
        GH.post_updateable_comment(
            comment_tags_and_bodies={
                "team_notification": "@ClickHouse/integrations team,  please, take a look"
            }
        )

    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
