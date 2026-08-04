import sys

from ci.praktika.gh import GH
from ci.praktika.info import Info

INTEGRATIONS_ECOSYSTEM_FILES = ("src/Core/TypeId.h",)

DOCS_PREFIX = "docs/"
CLICKPIPES_DOCS_PREFIX = "docs/integrations/clickpipes/"
INTEGRATIONS_DOCS_PREFIXES = (
    "docs/integrations/language-clients/",
    "docs/integrations/connectors/",
)

DOCS_TEAM = "@ClickHouse/docs"
CLICKPIPES_TEAM = "@ClickHouse/clickpipes"
INTEGRATIONS_ECOSYSTEM_TEAM = "@ClickHouse/integrations-ecosystem"
INTEGRATIONS_TEAM = "@ClickHouse/integrations"


def normalize_path(file):
    return file.removeprefix(".").removeprefix("/")


def get_teams_to_notify(changed_files):
    files = [normalize_path(file) for file in changed_files]
    teams = []

    if any(
        file.startswith(prefix)
        for file in files
        for prefix in INTEGRATIONS_ECOSYSTEM_FILES
    ):
        teams.append(INTEGRATIONS_TEAM)

    if not files or not all(file.startswith(DOCS_PREFIX) for file in files):
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
    teams = get_teams_to_notify(changed_files)
    if teams:
        team_word = "team" if len(teams) == 1 else "teams"
        GH.post_updateable_comment(
            comment_tags_and_bodies={
                "team_notification": (
                    f"{' '.join(teams)} {team_word}, please take a look"
                )
            }
        )

    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
