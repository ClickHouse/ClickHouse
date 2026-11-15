from praktika.gh import GH
from praktika.result import Result

# Sets dummy commit status for "CH Inc Sync" in the Merge Queue workflow

SYNC = "CH Inc sync"

if __name__ == "__main__":
    GH.post_commit_status(
        name=SYNC, status=Result.Status.OK, description="dummy status to enable merge", url=""
    )
