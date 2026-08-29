"""Guards that a documentation review request cannot be thrown away.

`Documentation Team Review Requests` starts one run per `pull_request_target`
event, but only one event per pull request arms real work (`opened` internally,
`labeled can be tested` on a fork); every other event produces a run that skips.
GitHub delivers it once and nothing re-sends it, so the runs at one head are not
interchangeable: these two settings decide whether the arming one is discarded.
"""

import os

import yaml

WORKFLOW = os.path.join(
    os.path.dirname(__file__), "../../.github/workflows/team_review_requests.yml"
)


def _concurrency():
    with open(WORKFLOW, encoding="utf-8") as f:
        return yaml.safe_load(f)["concurrency"]


def test_a_queued_run_is_not_discarded():
    assert _concurrency().get("queue") == "max", (
        "queue must be 'max': the default 'single' allows one pending run per "
        "group and cancels the one already waiting to make room for it"
    )


def test_a_running_run_is_not_cancelled():
    assert _concurrency().get("cancel-in-progress") is not True, (
        "cancel-in-progress must not be true: a later event of the same pull "
        "request would kill the arming run while it is still executing"
    )
