#!/usr/bin/env python3

"""
A trivial stateless slack bot that notifies about new broken tests in ClickHouse CI.
It checks what happened to our CI during the last check_period hours (1 hour) and
  notifies us in slack if necessary.
This script should be executed once each check_period hours (1 hour).
It will post duplicate messages if you run it more often; it will lose some messages
  if you run it less often.

You can run it locally with no arguments, it will work in a dry-run mode.
  Or you can set your own SLACK_URL_DEFAULT.
Feel free to add more checks, more details to messages, or better heuristics.

It's deployed to slack-bot-ci-lambda in CI/CD account

See also: https://aretestsgreenyet.com/
"""

import base64
import json
import os
import random

import requests

DRY_RUN_MARK = "<no url, dry run>"

MAX_FAILURES_DEFAULT = 30
SLACK_URL_DEFAULT = DRY_RUN_MARK

FLAKY_ALERT_PROBABILITY = 0.50
REPORT_NO_FAILURES_PROBABILITY = 0.99

MAX_TESTS_TO_REPORT = 4

# Slack has a stupid limitation on message size, it splits long messages into multiple,
# ones breaking formatting
MESSAGE_LENGTH_LIMIT = 4000

# Find tests that failed in master during the last check_period * 24 hours,
# but did not fail during the last 2 weeks. Assuming these tests were broken recently.
# Counts number of failures in check_period and check_period * 24 time windows
# to distinguish rare flaky tests from completely broken tests
NEW_BROKEN_TESTS_QUERY = """
WITH
    1 AS check_period,
    check_period * 24 AS extended_check_period,
    now() as now
SELECT
    test_name,
    any(report_url),
    countIf((check_start_time + check_duration_ms / 1000) < now - INTERVAL check_period HOUR) AS count_prev_periods,
    countIf((check_start_time + check_duration_ms / 1000) >= now - INTERVAL check_period HOUR) AS count
FROM checks
WHERE 1
    AND check_start_time BETWEEN now - INTERVAL 1 WEEK AND now
    AND (check_start_time + check_duration_ms / 1000) >= now - INTERVAL extended_check_period HOUR
    AND pull_request_number = 0
    AND test_status LIKE 'F%'
    AND check_status != 'success'
    AND test_name NOT IN (
        SELECT test_name FROM checks WHERE 1
        AND check_start_time >= now - INTERVAL 1 MONTH
        AND (check_start_time + check_duration_ms / 1000) BETWEEN now - INTERVAL 2 WEEK AND now - INTERVAL extended_check_period HOUR
        AND pull_request_number = 0
        AND check_status != 'success'
        AND test_status LIKE 'F%')
    AND test_context_raw NOT LIKE '%CannotSendRequest%' and test_context_raw NOT LIKE '%Server does not respond to health check%'
GROUP BY test_name
ORDER BY (count_prev_periods + count) DESC
"""

# Returns total number of failed checks during the last 24 hours
# and previous value of that metric (check_period hours ago)
COUNT_FAILURES_QUERY = """
WITH
    1 AS check_period,
    '%' AS check_name_pattern,
    now() as now
SELECT
    countIf((check_start_time + check_duration_ms / 1000) >= now - INTERVAL 24 HOUR) AS new_val,
    countIf((check_start_time + check_duration_ms / 1000) <= now - INTERVAL check_period HOUR) AS prev_val
FROM checks
WHERE 1
    AND check_start_time >= now - INTERVAL 1 WEEK
    AND (check_start_time + check_duration_ms / 1000) >= now - INTERVAL 24 + check_period HOUR
    AND pull_request_number = 0
    AND test_status LIKE 'F%'
    AND check_status != 'success'
    AND check_name ILIKE check_name_pattern
"""

# Returns percentage of failed checks (once per day, at noon)
FAILED_CHECKS_PERCENTAGE_QUERY = """
SELECT if(toHour(now('Europe/Amsterdam')) = 12, v, 0)
FROM
(
    SELECT
        countDistinctIf((commit_sha, check_name), (test_status LIKE 'F%') AND (check_status != 'success'))
            / countDistinct((commit_sha, check_name)) AS v
    FROM checks
    WHERE 1
        AND (pull_request_number = 0)
        AND (test_status != 'SKIPPED')
        AND (check_start_time > (now() - toIntervalDay(1)))
)
"""

# It shows all recent failures of the specified test (helps to find when it started)
ALL_RECENT_FAILURES_QUERY = """
WITH
    '{}' AS name_substr,
    90 AS interval_days,
    ('Stateless tests (asan)', 'Stateless tests (address)', 'Stateless tests (address, actions)', 'Integration tests (asan) [1/3]', 'Stateless tests (tsan) [1/3]') AS backport_and_release_specific_checks
SELECT
    toStartOfDay(check_start_time) AS d,
    count(),
    groupUniqArray(pull_request_number) AS prs,
    any(report_url)
FROM checks
WHERE ((now() - toIntervalDay(interval_days)) <= check_start_time) AND (pull_request_number NOT IN (
    SELECT pull_request_number AS prn
    FROM checks
    WHERE (prn != 0) AND ((now() - toIntervalDay(interval_days)) <= check_start_time) AND (check_name IN (backport_and_release_specific_checks))
)) AND (position(test_name, name_substr) > 0) AND (test_status IN ('FAIL', 'ERROR', 'FLAKY'))
GROUP BY d
ORDER BY d DESC
"""

SLACK_MESSAGE_JSON = {"type": "mrkdwn", "text": None}


def get_play_url(query):
    return (
        "https://play.clickhouse.com/play?user=play#"
        + base64.b64encode(query.encode()).decode()
    )


def run_clickhouse_query(query):
    url = "https://play.clickhouse.com/?user=play&query=" + requests.compat.quote(query)
    res = requests.get(url, timeout=30)
    if res.status_code != 200:
        print("Failed to execute query: ", res.status_code, res.content)
        res.raise_for_status()

    lines = res.text.strip().splitlines()
    return [x.split("\t") for x in lines]


def split_broken_and_flaky_tests(failed_tests):
    if not failed_tests:
        return None

    broken_tests = []
    flaky_tests = []
    for name, report, count_prev_str, count_str in failed_tests:
        count_prev, count = int(count_prev_str), int(count_str)
        if (count_prev < 2 <= count) or (count_prev == count == 1):
            # It failed 2 times or more within extended time window, it's definitely broken.
            # 2 <= count means that it was not reported as broken on previous runs
            broken_tests.append([name, report])
        elif 0 < count and count_prev == 0:
            # It failed only once, can be a rare flaky test
            flaky_tests.append([name, report])

    return broken_tests, flaky_tests


def format_failed_tests_list(failed_tests, failure_type):
    if len(failed_tests) == 1:
        res = f"There is a new {failure_type} test:\n"
    else:
        res = f"There are {len(failed_tests)} new {failure_type} tests:\n"

    for name, report in failed_tests[:MAX_TESTS_TO_REPORT]:
        cidb_url = get_play_url(ALL_RECENT_FAILURES_QUERY.format(name))
        res += f"-   *{name}*  -  <{report}|Report>  -  <{cidb_url}|CI DB> \n"

    if MAX_TESTS_TO_REPORT < len(failed_tests):
        res += (
            f"-   and {len(failed_tests) - MAX_TESTS_TO_REPORT} other "
            "tests... :this-is-fine-fire:"
        )

    return res


def get_new_broken_tests_message(failed_tests):
    if not failed_tests:
        return None

    broken_tests, flaky_tests = split_broken_and_flaky_tests(failed_tests)
    if len(broken_tests) == 0 and len(flaky_tests) == 0:
        return None

    msg = ""
    if len(broken_tests) > 0:
        msg += format_failed_tests_list(broken_tests, "*BROKEN*")
    elif random.random() > FLAKY_ALERT_PROBABILITY:
        looks_like_fuzzer = [x[0].count(" ") > 2 for x in flaky_tests]
        if not any(looks_like_fuzzer):
            print("Will not report flaky tests to avoid noise: ", flaky_tests)
            return None

    if len(flaky_tests) > 0:
        if len(msg) > 0:
            msg += "\n"
        msg += format_failed_tests_list(flaky_tests, "flaky")

    return msg


def get_too_many_failures_message_impl(failures_count):
    MAX_FAILURES = int(os.environ.get("MAX_FAILURES", MAX_FAILURES_DEFAULT))
    curr_failures = int(failures_count[0][0])
    prev_failures = int(failures_count[0][1])
    if curr_failures == 0 and prev_failures != 0:
        if random.random() < REPORT_NO_FAILURES_PROBABILITY:
            return None
        return "Wow, there are *no failures* at all... 0_o"
    return_none = (
        curr_failures < MAX_FAILURES
        or curr_failures < prev_failures
        or (curr_failures - prev_failures) / prev_failures < 0.2
    )
    if return_none:
        return None
    if prev_failures < MAX_FAILURES:
        return f":alert: *CI is broken: there are {curr_failures} failures during the last 24 hours*"
    return "CI is broken and it's getting worse: there are {curr_failures} failures during the last 24 hours"


def get_too_many_failures_message(failures_count):
    msg = get_too_many_failures_message_impl(failures_count)
    if msg:
        msg += "\nSee https://aretestsgreenyet.com/"
    return msg


def get_failed_checks_percentage_message(percentage):
    p = float(percentage[0][0]) * 100

    # Always report more than 1% of failed checks
    # For <= 1%: higher percentage of failures == higher probability
    if p <= random.random():
        return None

    msg = ":alert: " if p > 1 else "Only " if p < 0.5 else ""
    msg += f"*{p:.2f}%* of all checks in master have failed yesterday"
    return msg


def split_slack_message(long_message):
    lines = long_message.split("\n")
    messages = []
    curr_msg = ""
    for line in lines:
        if len(curr_msg) + len(line) < MESSAGE_LENGTH_LIMIT:
            curr_msg += "\n"
            curr_msg += line
        else:
            messages.append(curr_msg)
            curr_msg = line
    messages.append(curr_msg)
    return messages


def send_to_slack_impl(message):
    SLACK_URL = os.environ.get("SLACK_URL", SLACK_URL_DEFAULT)
    if SLACK_URL == DRY_RUN_MARK:
        return

    payload = SLACK_MESSAGE_JSON.copy()
    payload["text"] = message
    res = requests.post(SLACK_URL, json.dumps(payload), timeout=30)
    if res.status_code != 200:
        print("Failed to send a message to Slack: ", res.status_code, res.content)
        res.raise_for_status()


def send_to_slack(message):
    messages = split_slack_message(message)
    for msg in messages:
        send_to_slack_impl(msg)


def query_and_alert_if_needed(query, get_message_func):
    query_res = run_clickhouse_query(query)
    print("Got result {} for query {}", query_res, query)
    msg = get_message_func(query_res)
    if msg is None:
        return

    msg += f"\nCI DB query: <{get_play_url(query)}|link>"
    print("Sending message to slack:", msg)
    send_to_slack(msg)


def check_and_alert():
    query_and_alert_if_needed(NEW_BROKEN_TESTS_QUERY, get_new_broken_tests_message)
    query_and_alert_if_needed(COUNT_FAILURES_QUERY, get_too_many_failures_message)
    query_and_alert_if_needed(
        FAILED_CHECKS_PERCENTAGE_QUERY, get_failed_checks_percentage_message
    )


def handler(event, context):
    _, _ = event, context
    try:
        check_and_alert()
        return {"statusCode": 200, "body": "OK"}
    except Exception as e:
        send_to_slack(
            "I failed, please help me "
            f"(see ClickHouse/ClickHouse/tests/ci/slack_bot_ci_lambda/app.py): {e}"
        )
        return {"statusCode": 200, "body": "FAIL"}


if __name__ == "__main__":
    check_and_alert()
