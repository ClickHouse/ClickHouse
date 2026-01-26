import sys
from ci.praktika.info import Info
from ci.praktika.utils import Shell


if __name__ == "__main__":
    info = Info()
    if info.workflow_name == "BackportPR":
        num_commits = int(
            Shell.get_output_or_raise(
                f"git rev-list --count {info.base_branch}..{info.sha}"
            )
        )
        if num_commits == 0:
            print(f"ERROR: No commits found between {info.base_branch} and {info.sha}")
            sys.exit(-1)

        if num_commits > 50:
            print(
                f"ERROR: Number of commits between {info.sha} and {info.base_branch} is {num_commits}. "
                f"Backport PR should have between 1 and 50 commits."
            )
            sys.exit(-1)

        if Shell.check(f"git diff --quiet {info.base_branch}...{info.sha}"):
            print(f"ERROR: Diff is empty between {info.base_branch} and {info.sha}")
            sys.exit(-1)
    else:
        assert False, f"Unsupported workflow name [{info.workflow_name}]"
