from praktika.gh import GH

# Sets dummy commit status for "CH Inc Sync" in the Merge Queue workflow

SYNC = "CH Inc sync"

if __name__ == "__main__":
    GH.post_commit_status(
        name=SYNC, status="success", description="dummy status to enable merge", url=""
    )
