import sys

from ci.praktika.gh import GH
from ci.praktika.info import Info

integrations_ecosystem_files = ["src/Core/TypeId.h"]


def check():
    info = Info()

    changed_files = info.get_kv_data("changed_files")
    for file in changed_files:
        if any(file.startswith(f) for f in integrations_ecosystem_files):
            GH.post_updateable_comment(
                comment_tags_and_bodies={
                    "team_notification": "@ClickHouse/integrations team,  please, take a look"
                }
            )
            break

    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
