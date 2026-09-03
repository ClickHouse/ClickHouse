"""Upload PromQL compliance JSON to S3 after each Integration tests batch that ran it.

Runs as a post-hook after each Integration tests batch. Only the batch that
executed ``test_compliance.py`` leaves the JSON path given by
``COMPLIANCE_RESULT_FILE`` (default ``./ci/tmp/promql_compliance_result.json``,
same default as ``integration_test_job.py``).

* **Master** (upstream): uploads to ``REFs/master/<sha>/promql_compliance/`` so PRs
  can use it as baseline (same path as before).

* **PRs**: uploads to ``PRs/<pr>/<sha>/promql_compliance/`` for the dedicated
  ``PromQL Compliance`` job to fetch and post the GitHub comment (which requires
  ``enable_gh_auth`` — see ``promql_compliance_job.py``).

Does not call GitHub; integration-test runners do not have ``gh`` credentials.
"""

from __future__ import annotations

import os
import traceback
from pathlib import Path

from ci.jobs.scripts.job_hooks.promql_compliance_s3 import (
    should_publish_master_baseline,
    upload_master_result,
    upload_pr_result,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info

RESULT_FILE = Path(
    os.environ.get(
        "COMPLIANCE_RESULT_FILE",
        os.path.join(".", "ci", "tmp", "promql_compliance_result.json"),
    )
)


def check() -> None:
    try:
        _check_impl()
    except Exception:
        print("ERROR: PromQL compliance upload hook failed")
        traceback.print_exc()
        raise


def _check_impl() -> None:
    if not RESULT_FILE.is_file():
        print(
            f"PromQL compliance upload hook: no result at {RESULT_FILE} "
            "(batch did not run test_compliance), skip"
        )
        return

    info = Info()

    if should_publish_master_baseline(info):
        sha = (info.sha or "").strip()
        if len(sha) == 40:
            upload_master_result(RESULT_FILE, sha)
        else:
            print(
                f"PromQL compliance upload hook: skip S3 upload, unexpected SHA [{sha!r}]"
            )
        print("PromQL compliance upload hook: master / non-PR run, done")
        return

    if info.pr_number > 0:
        sha = (info.sha or "").strip()
        if len(sha) != 40:
            print(
                f"PromQL compliance upload hook: skip PR upload, unexpected SHA [{sha!r}]"
            )
            return
        url = upload_pr_result(RESULT_FILE, info.pr_number, sha)
        if url is None and Labels.COMP_PROMQL in list(info.pr_labels or []):
            raise RuntimeError(
                f"PromQL compliance upload hook: failed to upload PR result to S3 "
                f"(PR #{info.pr_number}, sha {sha[:9]}); "
                "downstream PromQL Compliance job will see 404"
            )
        return

    print("PromQL compliance upload hook: not a PR run, skip PR upload")


if __name__ == "__main__":
    check()
