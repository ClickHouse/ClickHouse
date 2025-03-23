import sys

from praktika.info import Info
from praktika.utils import Shell

sys.path.append("./tests/ci")
from digest_helper import DockerDigester

# TODO: script ensures seamless migration to praktika
#  it stores docker digests in old format for legacy ci jobs, as a praktika pipeline pre-hook
#  to be removed once migrated

if __name__ == "__main__":
    info = Info()

    # store docker digests
    image_to_digest_map = DockerDigester().get_all_digests()
    info.store_custom_data(key="digest_dockers", value=image_to_digest_map)

    # store changed files
    if info.pr_number > 0:
        changed_files_str = Shell.get_output(
            f"gh pr view {info.pr_number} --repo {info.repo_name} --json files --jq '.files[].path'",
            strict=True,
        )
    else:
        changed_files_str = Shell.get_output(
            f"gh api repos/{info.repo_name}/commits/{info.sha} | jq -r '.files[].filename'",
        )
    if changed_files_str:
        changed_files = changed_files_str.split("\n")
        info.store_custom_data("changed_files", changed_files)
