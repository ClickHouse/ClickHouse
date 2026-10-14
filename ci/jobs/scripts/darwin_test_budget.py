import json

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.settings.settings import SECRET_CI_DB_CONNECTION

CHECK_NAME = "Fast test (arm_darwin)"
RUNNER_LABEL = "macos-m2"
# The pool is sized so that every job in the queue finishes within this time.
QUEUE_DRAIN_SECONDS = 3 * 3600

# GitHub sends no event while a job waits, so the window must cover the longest wait.
QUEUE_QUERY = f"""
SELECT countIf(last_status = 'queued'), countIf(last_status = 'in_progress')
FROM
(
    SELECT argMax(status, updated_at) AS last_status
    FROM default.workflow_jobs
    WHERE has(labels, '{RUNNER_LABEL}') AND updated_at > now() - INTERVAL 24 HOUR
    GROUP BY id
)
FORMAT TSV
"""

LOAD_QUERY = f"""
SELECT median(test_seconds / wall)
FROM
(
    SELECT
        max(check_duration_ms) / 1000 AS wall,
        sumIf(test_duration_ms, test_name != '') / 1000 AS test_seconds
    FROM default.checks
    WHERE check_name = '{CHECK_NAME}' AND check_start_time > now() - INTERVAL 7 DAY
    GROUP BY pull_request_number, commit_sha, check_start_time
    HAVING countIf(test_status = 'OK') > 1000
)
FORMAT TSV
"""

# A test with no `OK` run this week, such as one the budget skipped all week, is absent and always runs.
DURATIONS_QUERY = f"""
SELECT test_name, median(test_duration_ms) / 1000 AS seconds
FROM default.checks
WHERE check_name = '{CHECK_NAME}' AND check_start_time > now() - INTERVAL 7 DAY
    AND test_name != '' AND test_status = 'OK'
GROUP BY test_name
ORDER BY seconds, test_name
FORMAT TSV
"""


def tests_to_skip():
    """Return the slowest tests that do not fit the time budget; empty when no job waits for a macOS runner.

    Every queued and running job must finish within `QUEUE_DRAIN_SECONDS`, so each gets that time
    multiplied by running / (running + queued). The average load (test-seconds per wall second)
    converts it into test-seconds, filled with the quickest tests by last week's median duration.
    Tests without an `OK` run last week, new or skipped all week, always run.
    """
    conn = json.loads(Info().get_secret(SECRET_CI_DB_CONNECTION).get_value())
    cidb = CIDB(url=conn["url"], user=conn["user"], passwd=conn["password"])
    queued, running = map(int, cidb.query(QUEUE_QUERY).split())
    print(f"macOS runner queue: {queued} queued, {running} in progress")
    if not queued:
        return []
    load = float(cidb.query(LOAD_QUERY))
    running = max(running, 1)
    wall = QUEUE_DRAIN_SECONDS * running / (running + queued)
    budget = wall * load
    print(f"Budget: {wall:.0f} s at load {load:.1f} = {budget:.0f} test-seconds")
    skip = []
    for line in cidb.query(DURATIONS_QUERY).splitlines():
        name, seconds = line.split("\t")
        budget -= float(seconds)
        if budget < 0:
            skip.append(name)
    print(f"Skip {len(skip)} slowest tests to fit the budget")
    return skip
