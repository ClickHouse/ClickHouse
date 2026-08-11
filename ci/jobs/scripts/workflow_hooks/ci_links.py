import tempfile
import traceback
from urllib.parse import quote_plus

from ci.praktika.gh import GH
from ci.praktika.info import Info

UPSTREAM_REPO = "ClickHouse/ClickHouse"
BLOCK_START = "<!-- CI automatic block start :ci_links: -->"
BLOCK_END = "<!-- CI automatic block end :ci_links: -->"


def get_sync_pr_search_url(pr_number):
    return (
        "https://github.com/search?q="
        + quote_plus(f"head:sync-upstream/pr/{pr_number} org:ClickHouse type:pr")
        + "&type=pullrequests"
    )


def has_block(body):
    return BLOCK_START in body and BLOCK_END in body


def append_block(body, block):
    if body.strip():
        return body.rstrip() + "\n\n" + block + "\n"
    return block + "\n"


def main():
    info = Info()
    if info.pr_number <= 0 or info.repo_name != UPSTREAM_REPO:
        print("NOTE: Not an upstream PR run - skip PR description update")
        return

    if has_block(info.pr_body or ""):
        print("NOTE: CI links already present - skip PR description update")
        return

    workflow_line = (
        f"Workflow [[{info.workflow_name}]({info.get_report_url(latest=True)})]"
    )
    sync_line = f"Sync PR [[sync-upstream/pr/{info.pr_number}]({get_sync_pr_search_url(info.pr_number)})]"
    block = f"{BLOCK_START}\n{workflow_line}\n{sync_line}\n{BLOCK_END}"

    title, body, _labels = GH.get_pr_title_body_labels()
    if not title:
        print("WARNING: Failed to fetch PR data - skip PR description update")
        return

    new_body = append_block(body or "", block)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", encoding="utf-8") as f:
        f.write(new_body)
        f.flush()
        if not GH.update_pr_body(body_file=f.name):
            print("WARNING: Failed to update PR description")
            return

    info.env.PR_BODY = new_body
    info.env.dump()
    print("PR description updated with CI links")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
