import re
from ci.praktika.info import Info

if __name__ == "__main__":
    info = Info()
    if info.pr_number == 0:
        # Extract original PR number from backport merge commits
        # Example: "Merge pull request #92596 from ClickHouse/backport/25.12/92538" -> extract 92538
        commit_message = info.commit_message
        match = re.search(r"backport/[^/]+/(\d+)", commit_message)
        if match:
            try:
                pr_number = int(match.group(1))
                info.set_parent_pr_number(pr_number)
            except ValueError as e:
                print(
                    f"Failed to get PR number from commit message [{commit_message}]: {e}"
                )
