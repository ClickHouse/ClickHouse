import json

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.settings.settings import SECRET_CI_DB_CONNECTION

CHECK_NAME = "Fast test (arm_darwin)"
RUNNER_LABEL = "macos-m2"
# A job never shrinks below this share of a full run, so the quickest tests always run.
MIN_SHARE = 0.25

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
SELECT median(wall), median(test_seconds)
FROM
(
    SELECT
        max(check_duration_ms) / 1000 AS wall,
        sumIf(test_duration_ms, test_name != '') / 1000 AS test_seconds
    FROM default.checks
    WHERE check_name = '{CHECK_NAME}' AND check_start_time > now() - INTERVAL 14 DAY
    GROUP BY pull_request_number, commit_sha, check_start_time
    HAVING countIf(test_status = 'OK') > 1000
)
FORMAT TSV
"""

DURATIONS_QUERY = f"""
SELECT test_name, median(test_duration_ms) / 1000 AS seconds
FROM default.checks
WHERE check_name = '{CHECK_NAME}' AND check_start_time > now() - INTERVAL 14 DAY
    AND test_name != '' AND test_status = 'OK'
GROUP BY test_name
ORDER BY seconds, test_name
FORMAT TSV
"""


def tests_to_skip():
    """Return the slowest tests that do not fit the time budget; empty when no job waits for a macOS runner.

    The budget is the median run time scaled by the share of the pool each job gets if every
    queued job starts now. The average load (test-seconds per wall second) converts it into
    test-seconds, filled with the quickest tests by their median CIDB duration. Tests without
    a CIDB duration, such as new ones, always run.
    """
    conn = json.loads(Info().get_secret(SECRET_CI_DB_CONNECTION).get_value())
    cidb = CIDB(url=conn["url"], user=conn["user"], passwd=conn["password"])
    queued, running = map(int, cidb.query(QUEUE_QUERY).split())
    print(f"macOS runner queue: {queued} queued, {running} in progress")
    if not queued:
        return []
    wall, test_seconds = map(float, cidb.query(LOAD_QUERY).split())
    load = test_seconds / wall
    running = max(running, 1)
    share = max(MIN_SHARE, running / (running + queued))
    budget = load * wall * share
    print(f"Budget: {share:.2f} of a {wall:.0f} s run at load {load:.1f} = {budget:.0f} test-seconds")
    skip = []
    for line in cidb.query(DURATIONS_QUERY).splitlines():
        name, seconds = line.split("\t")
        budget -= float(seconds)
        if budget < 0:
            skip.append(name)
    print(f"Skip {len(skip)} slowest tests to fit the budget")
    return skip
