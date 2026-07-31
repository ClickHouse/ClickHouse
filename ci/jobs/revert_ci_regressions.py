"""Hourly job: find failures that keep happening on `master`, work out which
pull request caused each of them, and revert that pull request.

The job takes the failures the CI database recorded for `master` in the last
day, groups them the way a human reading the "Failed checks" dashboard would --
by test name, and by check name for the failures that belong to no test -- and
keeps the groups that failed more than twice. A sporadic failure is not
interesting; a failure that repeats is either a regression somebody merged or a
test that has to be looked at anyway.

Every group is then handed to an AI agent (the `codex` CLI, the same one the
Code Review job runs). The agent has the repository, `gh`, and read-only access
to the CI database, and answers one question: did a recently merged pull request
introduce this failure, and which one. Its answer is a verdict with a confidence
and an explanation.

Only `regression` with `high` confidence acts. The offending pull request is
reverted, the revert is merged immediately with administrator privileges -- no
checks are awaited, because `master` is already broken and the revert restores
the state that was green -- and a draft pull request that reintroduces the
change is opened right after, so the author can fix the problem and bring the
change back through normal CI instead of recovering the diff by hand.

Every investigation is recorded in the `checks_investigated` table of the CI
database, the negative ones too: the table is the log of what the job looked at
and why it did or did not act, and it joins back to `checks` on test name, check
name, commit sha and pull request number.

Fail-closed throughout. An unreadable verdict, a revert that does not apply
cleanly, a pull request that is too old or is itself a revert, a failure that
was already handled -- each of those records a row and moves on rather than
guessing. At most `MAX_REVERTS_PER_RUN` reverts happen per run, so a systematic
misjudgement cannot empty `master` before somebody notices.

The revert branch name is shared with `.github/workflows/revert_broken_prs.yml`,
which reverts merges that landed with red CI -- a different signal, the same
remedy -- so that the two automations never revert one pull request twice.
"""

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from ci.defs.defs import BASE_BRANCH
from ci.praktika import Secret
from ci.praktika.cidb import CIDB
from ci.praktika.gh import GH
from ci.praktika.gh_auth import GHAuth
from ci.praktika.git import Git
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# How far back the failures are taken from, and how many times a failure has to
# repeat in that window to be investigated ("failed more than twice").
FAILURE_WINDOW_HOURS = 24
MIN_FAILURES = 3

# A failure stays inside the observation window for many hourly runs. Without a
# cooldown the same failure would be investigated every hour, which costs agent
# time and writes a row an hour saying the same thing.
INVESTIGATION_COOLDOWN_HOURS = 6

# Bounds per run. The job runs hourly, so it has to be finished well within the
# hour: overlapping runs would investigate the same failures and could revert
# the same pull request twice. Whatever does not fit is picked up by the next
# run, most frequent failure first. The revert budget is what stands between a
# systematically wrong agent and an emptied master; at an hourly cadence, two
# reverts per run is more throughput than a human on call would manage.
MAX_INVESTIGATIONS_PER_RUN = 4
MAX_REVERTS_PER_RUN = 2
RUN_BUDGET_SEC = 45 * 60

# A pull request merged longer ago than this is not reverted automatically even
# when the agent is certain: later changes are likely to depend on it, so the
# revert becomes a human decision. The investigation is still recorded, with the
# offending pull request named in it, so the finding is not lost.
MAX_CULPRIT_AGE_DAYS = 3

# Table in the CI database that records the investigations. It joins with
# `checks`: `test_name`, `check_name`, `check_names`, `commit_shas`,
# `report_url` and `offending_pull_request_number` all carry values from there.
INVESTIGATION_TABLE = "checks_investigated"

INVESTIGATION_TABLE_DDL = f"""\
CREATE TABLE IF NOT EXISTS {INVESTIGATION_TABLE}
(
    `investigation_time` DateTime COMMENT 'When the investigation ran',
    `task_url` String COMMENT 'The CI job that ran the investigation',
    `failure_kind` LowCardinality(String) COMMENT 'test: a failing test case. check: a failing CI check with no test attached',
    `test_name` LowCardinality(String) COMMENT 'checks.test_name, empty for a check-level failure',
    `check_name` LowCardinality(String) COMMENT 'checks.check_name, empty for a test-level failure, which is grouped across checks',
    `check_names` Array(LowCardinality(String)) COMMENT 'Every checks.check_name the failure was seen in',
    `failure_count` UInt32 COMMENT 'Occurrences within the observation window',
    `first_failure_time` DateTime COMMENT 'First occurrence within the observation window',
    `last_failure_time` DateTime COMMENT 'Last occurrence within the observation window',
    `commit_shas` Array(LowCardinality(String)) COMMENT 'checks.commit_sha values the failure was seen on',
    `report_url` String COMMENT 'checks.report_url of the most recent occurrence',
    `verdict` LowCardinality(String) COMMENT 'regression, not_a_regression, inconclusive, or error when the investigation itself failed',
    `confidence` LowCardinality(String) COMMENT 'high, medium or low',
    `offending_pull_request_number` UInt32 COMMENT 'checks.pull_request_number of the pull request that introduced the failure, 0 if none was found',
    `offending_commit_sha` LowCardinality(String) COMMENT 'checks.commit_sha of the master commit that introduced the failure',
    `explanation` String COMMENT 'Why the agent came to this verdict, and why the job did or did not act on it',
    `action` LowCardinality(String) COMMENT 'What the job did about it',
    `revert_pull_request_number` UInt32 COMMENT 'The revert that was created and merged, 0 if none',
    `reintroduce_pull_request_number` UInt32 COMMENT 'The draft pull request reintroducing the change, 0 if none'
)
ENGINE = ReplicatedMergeTree
ORDER BY (investigation_time, failure_kind, test_name, check_name)
"""

# Branch names. `revert-<pr>` is what `.github/workflows/revert_broken_prs.yml`
# uses as well, deliberately: whichever automation gets there first, the other
# one finds the branch (or the pull request opened from it) and stands down.
REVERT_BRANCH_PREFIX = "revert-"
REINTRODUCE_BRANCH_PREFIX = "reapply-"

ROBOT_NAME = "robot-clickhouse"
ROBOT_EMAIL = "robot-clickhouse@users.noreply.github.com"

# OpenAI API key for the codex CLI, the same secret the Code Review job uses.
OPENAI_KEY_SECRET = "/ci/llm/openai_api_key"
MAX_AGENT_ATTEMPTS = 2
AGENT_TIMEOUT_SEC = 12 * 60

TEMP_DIR = "./ci/tmp"
# How much of the recorded failure output is put in front of the agent. The full
# text can be a megabyte of stress-test log; the head is what identifies it.
CONTEXT_LIMIT = 4000

VERDICTS = ("regression", "not_a_regression", "inconclusive")
CONFIDENCES = ("high", "medium", "low")


class Action:
    """What the job did about an investigated failure."""

    NONE = "none"
    REVERTED = "reverted"
    REVERT_CONFLICT = "revert_conflict"
    REVERT_FAILED = "revert_failed"
    ALREADY_REVERTED = "already_reverted"
    REVERT_IN_FLIGHT = "revert_in_flight"
    SKIPPED_GUARD = "skipped_guard"
    SKIPPED_LIMIT = "skipped_limit"


class RevertConflict(Exception):
    """The revert does not apply cleanly on the current base branch."""


@dataclass
class Failure:
    """A group of CI failures on master that repeated within the window."""

    failure_kind: str
    test_name: str
    check_name: str
    check_names: List[str]
    failure_count: int
    first_failure_time: str
    last_failure_time: str
    commit_shas: List[str]
    report_url: str
    context: str = ""

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.failure_kind, self.test_name, self.check_name)

    @property
    def title(self) -> str:
        return self.test_name or self.check_name

    @property
    def markdown(self) -> str:
        if self.failure_kind == "test":
            return f"test `{self.test_name}`"
        return f"check `{self.check_name}`"

    @classmethod
    def from_row(cls, row: dict) -> "Failure":
        return cls(
            failure_kind=row["failure_kind"],
            test_name=row["test_name"],
            check_name=row["check_name"],
            check_names=list(row.get("check_names") or []),
            failure_count=int(row["failure_count"]),
            first_failure_time=row["first_failure_time"],
            last_failure_time=row["last_failure_time"],
            commit_shas=list(row.get("commit_shas") or []),
            report_url=row.get("report_url", ""),
            context=row.get("context", ""),
        )


@dataclass
class Investigation:
    """One row of `checks_investigated`."""

    failure: Failure
    verdict: str = "error"
    confidence: str = ""
    offending_pull_request_number: int = 0
    offending_commit_sha: str = ""
    explanation: str = ""
    action: str = Action.NONE
    revert_pull_request_number: int = 0
    reintroduce_pull_request_number: int = 0

    def is_actionable(self) -> bool:
        """Whether the verdict is certain enough to revert on."""
        return (
            self.verdict == "regression"
            and self.confidence == "high"
            and self.offending_pull_request_number > 0
        )

    def note(self, text: str) -> None:
        """Append to the explanation what the job decided to do about it."""
        self.explanation = f"{self.explanation}\n\n{text}".strip()

    def to_record(self, investigation_time: str, task_url: str) -> dict:
        failure = self.failure
        return {
            "investigation_time": investigation_time,
            "task_url": task_url,
            "failure_kind": failure.failure_kind,
            "test_name": failure.test_name,
            "check_name": failure.check_name,
            "check_names": failure.check_names,
            "failure_count": failure.failure_count,
            "first_failure_time": failure.first_failure_time,
            "last_failure_time": failure.last_failure_time,
            "commit_shas": failure.commit_shas,
            "report_url": failure.report_url,
            "verdict": self.verdict,
            "confidence": self.confidence,
            "offending_pull_request_number": self.offending_pull_request_number,
            "offending_commit_sha": self.offending_commit_sha,
            "explanation": self.explanation,
            "action": self.action,
            "revert_pull_request_number": self.revert_pull_request_number,
            "reintroduce_pull_request_number": self.reintroduce_pull_request_number,
        }


def failures_query(
    hours=FAILURE_WINDOW_HOURS, min_failures=MIN_FAILURES, context_limit=CONTEXT_LIMIT
) -> str:
    """Failures on master that repeated within the window, grouped the way the
    "Failed checks" dashboard groups them.

    Two kinds of rows are counted. Test-level rows are the failing test cases: a
    `test_status` of `FAIL`/`FAILURE`/`ERROR` inside a check that did not
    succeed, grouped by `test_name` across every check the test failed in -- one
    test failing in both the debug and the tsan build is one failure. Check-level
    rows are the failures no test carries: a check that ended in `failure` or
    `error` with no test name, which is how a build failure, a server that would
    not start, or a job that ran out of time is recorded. Those are grouped by
    `check_name`. A `skipped` check is not a failure and is not counted.

    The group columns are named apart from the source columns (`group_test`,
    `group_check`): aliasing `'' AS test_name` next to a real `test_name` would
    make the alias shadow the column in the same `SELECT`.

    `toUInt32` keeps the counters out of JSON 64-bit integer quoting, and the
    format is fixed in the query so the caller does not depend on server-side
    format settings.
    """
    table = Settings.CI_DB_TABLE_NAME
    master_runs = (
        f"check_start_time >= now() - INTERVAL {int(hours)} HOUR\n"
        f"        AND head_ref = '{BASE_BRANCH}'\n"
        f"        AND startsWith(head_repo, 'ClickHouse/')"
    )
    return f"""\
WITH failures AS
(
    SELECT
        'test' AS failure_kind,
        test_name AS group_test,
        '' AS group_check,
        check_name AS seen_in_check,
        commit_sha,
        check_start_time,
        report_url,
        test_context_raw
    FROM {table}
    WHERE {master_runs}
        AND test_status != 'SKIPPED'
        AND (test_status LIKE 'F%' OR test_status LIKE 'E%')
        AND check_status != 'success'
    UNION ALL
    SELECT
        'check' AS failure_kind,
        '' AS group_test,
        check_name AS group_check,
        check_name AS seen_in_check,
        commit_sha,
        check_start_time,
        report_url,
        test_context_raw
    FROM {table}
    WHERE {master_runs}
        AND test_name = ''
        AND check_status IN ('failure', 'error')
)
SELECT
    failure_kind,
    group_test AS test_name,
    group_check AS check_name,
    toUInt32(count()) AS failure_count,
    toString(min(check_start_time)) AS first_failure_time,
    toString(max(check_start_time)) AS last_failure_time,
    arraySort(groupUniqArray(20)(seen_in_check)) AS check_names,
    arraySort(groupUniqArray(50)(commit_sha)) AS commit_shas,
    argMax(report_url, check_start_time) AS report_url,
    substring(argMax(test_context_raw, check_start_time), 1, {int(context_limit)}) AS context
FROM failures
GROUP BY failure_kind, group_test, group_check
HAVING failure_count >= {int(min_failures)}
ORDER BY failure_count DESC, test_name, check_name
FORMAT JSONEachRow
"""


def recent_investigations_query(hours=FAILURE_WINDOW_HOURS) -> str:
    """When each failure was last investigated, and whether acting on it has
    already led to a revert within the window."""
    return f"""\
SELECT
    failure_kind,
    test_name,
    check_name,
    toString(max(investigation_time)) AS last_investigation_time,
    toUInt8(max(action = '{Action.REVERTED}')) AS reverted
FROM {INVESTIGATION_TABLE}
WHERE investigation_time >= now() - INTERVAL {int(hours)} HOUR
GROUP BY failure_kind, test_name, check_name
FORMAT JSONEachRow
"""


def parse_json_each_row(text: str) -> List[dict]:
    return [json.loads(line) for line in (text or "").splitlines() if line.strip()]


def parse_db_time(value: str) -> datetime:
    """Parse a `DateTime` the way the CI database renders it. The server runs in
    UTC and the value carries no zone, so it is attached here: comparing it with
    a naive local `now()` would be off by the runner's offset."""
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def skip_reason(
    failure: Failure, prior: Dict[Tuple[str, str, str], dict], now: datetime
) -> str:
    """Why this failure is not investigated in this run, or "" to investigate it.

    A failure stays in the observation window long after it has been dealt with,
    so a failure that was already reverted within the window is left alone
    entirely, and any other failure is re-investigated only once the cooldown has
    passed. A second opinion an hour later on the same evidence is worth neither
    the agent time nor the row.
    """
    seen = prior.get(failure.key)
    if not seen:
        return ""
    if int(seen.get("reverted") or 0):
        return "a revert for this failure was already created within the window"
    age = now - parse_db_time(seen["last_investigation_time"])
    if age < timedelta(hours=INVESTIGATION_COOLDOWN_HOURS):
        return (
            f"investigated {int(age.total_seconds() // 60)} minutes ago, "
            f"the cooldown is {INVESTIGATION_COOLDOWN_HOURS} hours"
        )
    return ""


def parse_verdict(text: str) -> dict:
    """Validate and normalize the agent's verdict. Raises `ValueError` on
    anything unexpected: a verdict that cannot be read is an error to record,
    never something to act on."""
    text = (text or "").strip()
    # The agent is asked for bare JSON but sometimes wraps it in a code fence.
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
    if fenced:
        text = fenced.group(1)
    if not text:
        raise ValueError("the agent wrote an empty verdict")
    try:
        verdict = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"the verdict is not valid JSON: {e}") from e
    if not isinstance(verdict, dict):
        raise ValueError(
            f"the verdict is not a JSON object but a {type(verdict).__name__}"
        )

    name = verdict.get("verdict")
    if name not in VERDICTS:
        raise ValueError(f"unknown verdict {name!r}, expected one of {list(VERDICTS)}")
    confidence = verdict.get("confidence")
    if confidence not in CONFIDENCES:
        raise ValueError(
            f"unknown confidence {confidence!r}, expected one of {list(CONFIDENCES)}"
        )
    explanation = str(verdict.get("explanation", "")).strip()
    if not explanation:
        raise ValueError("the verdict has no explanation")

    raw_number = verdict.get("offending_pull_request") or 0
    try:
        pull_request = int(raw_number)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f"offending_pull_request is not a number: {raw_number!r}"
        ) from e
    if pull_request < 0:
        raise ValueError(f"offending_pull_request is negative: {pull_request}")
    commit = str(verdict.get("offending_commit") or "").strip()
    if commit and not re.fullmatch(r"[0-9a-f]{7,40}", commit):
        raise ValueError(f"offending_commit is not a commit sha: {commit!r}")

    if name == "regression" and not pull_request:
        raise ValueError("the verdict is a regression but names no pull request")
    if name != "regression" and pull_request:
        # Naming a pull request while concluding this is not a regression is a
        # contradiction. The pull request is dropped so that nothing downstream
        # can act on it; the explanation is kept as written.
        pull_request = 0
        commit = ""

    return {
        "verdict": name,
        "confidence": confidence,
        "offending_pull_request_number": pull_request,
        "offending_commit_sha": commit,
        "explanation": explanation,
    }


def culprit_guard(pull_request: dict, failure: Failure, now: datetime) -> str:
    """Why the named pull request must not be reverted automatically, or "" if
    it may be. Everything here is checked against GitHub, not against what the
    agent claimed."""
    number = pull_request.get("number")
    state = str(pull_request.get("state", "")).lower()
    if state != "merged":
        return f"pull request #{number} is {state or 'in an unknown state'}, not merged"
    if pull_request.get("baseRefName") != BASE_BRANCH:
        return (
            f"pull request #{number} was merged into "
            f"{pull_request.get('baseRefName')!r}, not {BASE_BRANCH!r}"
        )
    if not (pull_request.get("mergeCommit") or {}).get("oid"):
        return f"pull request #{number} has no merge commit recorded"

    merged_at = pull_request.get("mergedAt") or ""
    if not merged_at:
        return f"pull request #{number} has no merge time recorded"
    merged = datetime.strptime(merged_at, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    if now - merged > timedelta(days=MAX_CULPRIT_AGE_DAYS):
        return (
            f"pull request #{number} was merged {(now - merged).days} days ago, longer "
            f"than the {MAX_CULPRIT_AGE_DAYS} days an automatic revert covers"
        )
    if merged > parse_db_time(failure.last_failure_time):
        return (
            f"pull request #{number} was merged at {merged_at}, after the last "
            f"occurrence of the failure at {failure.last_failure_time}"
        )

    # Never revert a revert: undoing one restores the very breakage the other
    # automation, or a human, removed, and two bots can otherwise flip a change
    # back and forth forever.
    title = pull_request.get("title") or ""
    body = pull_request.get("body") or ""
    head = pull_request.get("headRefName") or ""
    if title.lower().startswith(("revert", "reapply")):
        return f"pull request #{number} is itself a revert or a reapply ({title!r})"
    if "Reverts ClickHouse/" in body:
        return f"pull request #{number} is itself a revert of another one"
    if head.startswith((REVERT_BRANCH_PREFIX, REINTRODUCE_BRANCH_PREFIX)):
        return f"pull request #{number} comes from the automation branch {head!r}"
    return ""


def get_pull_request(number: int, repo: str) -> Optional[dict]:
    fields = "number,title,body,url,state,mergedAt,mergeCommit,baseRefName,headRefName,author"
    output = GH.get_output_with_retries(
        f"gh pr view {int(number)} --repo {shlex.quote(repo)} --json {fields}",
        verbose=True,
    )
    if not output.strip():
        return None
    return json.loads(output)


def already_handled(merge_commit: str, revert_branch: str, repo: str) -> str:
    """Why this merge has already been dealt with, or "" if it has not.

    Three independent checks, from the most immediate to the most delayed. The
    revert is already on master: `git revert` records the reverted sha in the
    message, so history is authoritative and has no indexing lag. The revert
    branch is already pushed: an in-flight revert, possibly by the workflow in
    `.github/workflows/revert_broken_prs.yml`. A revert pull request exists in
    any state: covers a merged revert whose branch was deleted afterwards.
    """
    if Shell.get_output(
        f"git log origin/{BASE_BRANCH} --fixed-strings "
        f"--grep={shlex.quote('This reverts commit ' + merge_commit)} --format=%H"
    ).strip():
        return f"the revert of {merge_commit} is already on {BASE_BRANCH}"
    if Shell.check(
        f"git ls-remote --exit-code --heads origin "
        f"{shlex.quote('refs/heads/' + revert_branch)}"
    ):
        return f"the revert branch {revert_branch} already exists on the remote"
    existing = GH.get_output_with_retries(
        f"gh pr list --repo {shlex.quote(repo)} --head {shlex.quote(revert_branch)} "
        "--state all --json number --jq '.[].number'"
    ).strip()
    if existing:
        return (
            f"a revert pull request already exists for {revert_branch}: "
            f"{' '.join(existing.split())}"
        )
    return ""


def investigation_prompt(failure: Failure, verdict_file: str) -> str:
    checks = ", ".join(f"`{name}`" for name in failure.check_names)
    commits = ", ".join(failure.commit_shas)
    what = (
        f"The test `{failure.test_name}` fails."
        if failure.failure_kind == "test"
        else f"The CI check `{failure.check_name}` fails without attributing the "
        f"failure to any test."
    )
    return f"""\
You are investigating a failure that keeps happening in ClickHouse CI on the `{BASE_BRANCH}` branch.

Answer one question: was this failure introduced by a pull request that was recently merged into
`{BASE_BRANCH}`, and if so, which one?

The failure:
- {what}
- Seen {failure.failure_count} times in the last {FAILURE_WINDOW_HOURS} hours on `{BASE_BRANCH}`,
  between {failure.first_failure_time} and {failure.last_failure_time} UTC.
- In these CI checks: {checks}
- On these `{BASE_BRANCH}` commits: {commits}
- Most recent CI report: {failure.report_url}
- Output recorded with the most recent occurrence (truncated):
```
{failure.context}
```

What you have:
- The repository, checked out at `{BASE_BRANCH}` with full history; `origin/{BASE_BRANCH}` is current.
- The `gh` CLI, authenticated for `ClickHouse/ClickHouse`.
- The CI results database, read-only, no credentials needed:
  `curl -sS 'https://play.clickhouse.com/?user=play' --data-binary "<SQL>"`
  Table `default.checks` holds one row per CI check plus one row per test case in it. Columns:
  `check_start_time DateTime`, `check_name`, `check_status` (`success`, `failure`, `error`,
  `skipped`, ...), `test_name`, `test_status` (`OK`, `FAIL`, `ERROR`, `SKIPPED`), `test_context_raw`
  (the recorded failure output), `report_url`, `commit_sha`, `pull_request_number` (0 for
  `{BASE_BRANCH}` runs), `head_ref`, `head_repo`. Runs on `{BASE_BRANCH}` are
  `head_ref = '{BASE_BRANCH}' AND startsWith(head_repo, 'ClickHouse/')`. Append `FORMAT PrettyCompact`
  or `FORMAT JSONEachRow`, and keep result sets small.

How to investigate:
1. Establish when the failure started. Query the last 30 days of `{BASE_BRANCH}` runs for this test
   or check, ordered by `check_start_time`, and find the earliest commit that failed and the last
   commit that passed before it.
2. Decide whether this is a regression at all. It is not one when:
   - the test or check has been failing on and off for a long time, which makes it flaky;
   - it also fails on pull requests that change nothing related;
   - the output shows an infrastructure problem: the runner ran out of memory or disk, a network,
     S3, docker or apt failure, a runner that disappeared, or a timeout of an already-slow test.
   Failures like these must not lead to a revert. Say so, and explain what you saw.
3. If it is a regression, take the commits between the last good and the first bad one
   (`git log --oneline <last_good>..<first_bad>`), map each to its pull request (the merge commit
   message carries `Merge pull request #N`), read the diffs with `git show`, `gh pr view` and
   `gh pr diff`, and find the one that explains this exact failure.
4. Read the real failure output before concluding: the `test_context_raw` column of the failing rows,
   or the report linked above. Your explanation has to name the mechanism -- which change makes which
   query, assertion or behaviour fail. A pull request merely being in the range is not an explanation,
   and neither is it touching a file with a similar name.

Be honest about how sure you are. A `regression` verdict with `high` confidence makes CI revert the
pull request and merge the revert immediately, without waiting for any checks. Use it only when all
of this holds:
- the failure is new -- the test or check passed consistently before it appeared;
- the first failing commit is identified and the range narrows down to one pull request;
- you can state the causal mechanism from the diff.
Otherwise answer with `medium` or `low` confidence, or with the `inconclusive` verdict. "I could not
tell" is a useful answer that costs nothing; a wrong revert throws away somebody's work and breaks
the branch a second time.

Change nothing: no commits, no pushes, no pull requests, no comments, no labels, no reviews, no edits
to tracked files. Investigate only. The one file you write is the verdict.

Write the verdict as JSON to `{verdict_file}`, and put nothing else in that file:
{{
  "verdict": "regression" | "not_a_regression" | "inconclusive",
  "confidence": "high" | "medium" | "low",
  "offending_pull_request": <the pull request number, or 0 when there is none>,
  "offending_commit": "<sha of the {BASE_BRANCH} commit that introduced the failure, or empty>",
  "explanation": "<a few sentences: what fails, when it started, and why that pull request causes it>"
}}
"""


def revert_body(pull_request: dict, investigation: Investigation) -> str:
    """The body of the revert pull request.

    The `Reverts ClickHouse/ClickHouse#N` line is the marker the rest of CI
    reads: it links the two pull requests, it is how the changelog job pairs a
    revert with what it reverts, and it is what tells the merge-readiness check
    that this is a revert."""
    failure = investigation.failure
    checks = "\n".join(f"- `{name}`" for name in failure.check_names)
    return f"""\
Reverts ClickHouse/ClickHouse#{pull_request["number"]}

This pull request was reverted automatically by the `Revert CI regressions` job, which found that it
introduced a failure that keeps happening on `{BASE_BRANCH}`, and merged this revert without waiting
for checks so that `{BASE_BRANCH}` is usable again.

**Failure:** {failure.markdown}

Seen {failure.failure_count} times in the last {FAILURE_WINDOW_HOURS} hours on `{BASE_BRANCH}`,
between {failure.first_failure_time} and {failure.last_failure_time} UTC, in:

{checks}

Most recent report: {failure.report_url}

**Why this pull request:**

{investigation.explanation}

A draft pull request reintroducing the change is opened right after this one is merged, so the change
can come back through normal CI once the failure is fixed. Every investigation this job makes,
including the ones that revert nothing, is recorded in the `{INVESTIGATION_TABLE}` table of the CI
database.

If this revert is wrong, say so here and bring the change back: the job errs towards reverting
because a broken `{BASE_BRANCH}` blocks everybody, not because its judgement beats yours.

### Changelog category (leave one):
- CI Fix or Improvement (changelog entry is not required)

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
"""


def reintroduce_body(
    pull_request: dict, revert_pull_request: int, investigation: Investigation
) -> str:
    number = pull_request["number"]
    failure = investigation.failure
    return f"""\
Related: https://github.com/ClickHouse/ClickHouse/pull/{number}
Related: https://github.com/ClickHouse/ClickHouse/pull/{revert_pull_request}

This reintroduces ClickHouse/ClickHouse#{number}, which the `Revert CI regressions` job reverted in
ClickHouse/ClickHouse#{revert_pull_request} because it introduced a failure on `{BASE_BRANCH}`.

Nothing is fixed here yet: this is the reverted change, unchanged, opened as a draft so that the work
is not lost and can come back through normal CI. Take it over -- fix the failure below, push to this
branch, and mark it ready for review.

**Failure to fix:** {failure.markdown}

Most recent report: {failure.report_url}

**Why the change was reverted:**

{investigation.explanation}

### Changelog category (leave one):
- Not for changelog (changelog entry is not required)

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
"""


def run_agent(prompt: str, verdict_file: str) -> str:
    """Run the codex agent once and return what it wrote to `verdict_file`.

    The same invocation as the Code Review job, which is what these runners are
    set up for: a writable workspace so the verdict file can be written, and
    network access for `gh` and for the CI database queries."""
    if os.path.exists(verdict_file):
        os.unlink(verdict_file)
    openai_key = Secret.Config(
        name=OPENAI_KEY_SECRET, type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    with tempfile.TemporaryDirectory(dir=TEMP_DIR) as codex_home:
        subprocess.run(
            ["codex", "login", "--with-api-key"],
            input=openai_key,
            text=True,
            check=True,
            env={**os.environ, "CODEX_HOME": codex_home},
        )
        Shell.check(
            f"CODEX_HOME={shlex.quote(codex_home)} codex exec "
            f"-m gpt-5.4 -c 'model_reasoning_effort=xhigh' "
            f"-s workspace-write "
            f"-c sandbox_workspace_write.network_access=true "
            f"-c approval_policy=never "
            f"--color never "
            f"{shlex.quote(prompt)}",
            verbose=True,
            timeout=AGENT_TIMEOUT_SEC,
        )
    if not os.path.exists(verdict_file):
        raise ValueError(f"the agent did not write {verdict_file}")
    with open(verdict_file, "r", encoding="utf-8") as fd:
        return fd.read()


def reset_worktree() -> None:
    """Put the checkout back on the current tip of the base branch with nothing
    left behind.

    Called before and after every agent run, and after every revert. The agent
    is told to change nothing, but it has a writable workspace, and a revert has
    to start from an untouched base branch. The branch is re-fetched every time
    because the job moves it itself: a second revert in the same run has to be
    built on top of the first one, not on the master the run started with.
    `ci/tmp` is spared, since that is where the job keeps its own state,
    including the verdict files."""
    Shell.check("git revert --abort", verbose=False)
    Shell.check(
        f"git fetch --no-tags --prune --no-recurse-submodules origin "
        f"+refs/heads/{BASE_BRANCH}:refs/remotes/origin/{BASE_BRANCH} && "
        f"git checkout --force --detach origin/{BASE_BRANCH} && "
        "git clean -fd --exclude=ci/tmp",
        verbose=True,
        strict=True,
    )


def merge_commit_parents(sha: str) -> int:
    line = Shell.get_output(
        f"git rev-list --parents -n 1 {shlex.quote(sha)}", strict=True
    )
    return len(line.split()) - 1


def pull_request_number_from_url(url: str) -> int:
    match = re.search(r"/pull/(\d+)", url or "")
    if not match:
        raise RuntimeError(f"cannot read a pull request number out of {url!r}")
    return int(match.group(1))


def write_temp(text: str) -> str:
    with tempfile.NamedTemporaryFile(
        "w", dir=TEMP_DIR, suffix=".md", delete=False, encoding="utf-8"
    ) as fd:
        fd.write(text)
        return fd.name


def create_pull_request(
    repo: str, branch: str, title: str, body: str, draft=False
) -> int:
    body_file = write_temp(body)
    try:
        url = Shell.get_output(
            f"gh pr create{' --draft' if draft else ''} --repo {shlex.quote(repo)} "
            f"--base {BASE_BRANCH} --head {shlex.quote(branch)} "
            f"--title {shlex.quote(title)} --body-file {shlex.quote(body_file)}",
            verbose=True,
            strict=True,
        ).strip()
    finally:
        os.unlink(body_file)
    return pull_request_number_from_url(url.splitlines()[-1] if url else "")


def merge_immediately(number: int, repo: str) -> None:
    """Merge the revert with administrator privileges, bypassing the required
    checks and the merge queue on the base branch.

    `gh pr merge` is retried internally, and a retry of an already merged pull
    request fails, so the outcome is decided by asking GitHub for the state
    rather than by the exit code of the last attempt."""
    GH.merge_pr(pr=number, repo=repo, admin=True)
    state = GH.get_output_with_retries(
        f"gh pr view {int(number)} --repo {shlex.quote(repo)} --json state --jq .state"
    ).strip()
    if state != "MERGED":
        raise RuntimeError(
            f"the revert pull request #{number} is {state or 'unreadable'} after merging it"
        )


def create_revert(
    pull_request: dict, merge_commit: str, investigation: Investigation, repo: str
) -> Tuple[str, int]:
    """Revert the merge, open the revert pull request, and merge it immediately.

    Returns the sha of the revert commit and the number of the revert pull
    request. Raises `RevertConflict` when the revert does not apply, which is not
    an error: it means the change can no longer be removed on its own and a human
    has to untangle it."""
    number = pull_request["number"]
    branch = f"{REVERT_BRANCH_PREFIX}{number}"
    Shell.check(
        f"git checkout -B {shlex.quote(branch)} origin/{BASE_BRANCH}",
        verbose=True,
        strict=True,
    )

    # A merge commit needs the mainline: reverting it means undoing what the
    # merged branch brought in relative to the base. A squashed or rebased merge
    # is an ordinary commit and takes no mainline.
    mainline = "-m 1 " if merge_commit_parents(merge_commit) > 1 else ""
    if not Shell.check(
        f"git revert {mainline}--no-edit {shlex.quote(merge_commit)}", verbose=True
    ):
        Shell.check("git revert --abort", verbose=True)
        raise RevertConflict(
            f"reverting {merge_commit} conflicts with later changes on {BASE_BRANCH}"
        )
    revert_commit = Shell.get_output("git rev-parse HEAD", strict=True).strip()

    if not Git.push(repo, f"HEAD:refs/heads/{branch}"):
        raise RuntimeError(f"failed to push {branch}")

    revert_pull_request = create_pull_request(
        repo,
        branch,
        f'Revert "{pull_request["title"]}"',
        revert_body(pull_request, investigation),
    )
    # Detach before merging: merging deletes the branch, including locally, and
    # git refuses to delete the branch that is checked out.
    Shell.check("git checkout --detach", verbose=True, strict=True)
    merge_immediately(revert_pull_request, repo)
    return revert_commit, revert_pull_request


def create_reintroduce(
    pull_request: dict,
    revert_commit: str,
    revert_pull_request: int,
    investigation: Investigation,
    repo: str,
) -> int:
    """Open the draft pull request that brings the reverted change back.

    Built on top of the revert commit, and opened only once the revert has been
    merged, so that the base branch already contains the revert and GitHub shows
    the reintroduced change as the diff instead of an empty one."""
    number = pull_request["number"]
    branch = f"{REINTRODUCE_BRANCH_PREFIX}{number}"
    Shell.check(
        f"git checkout -B {shlex.quote(branch)} {shlex.quote(revert_commit)}",
        verbose=True,
        strict=True,
    )
    if not Shell.check(
        f"git revert --no-edit {shlex.quote(revert_commit)}", verbose=True
    ):
        Shell.check("git revert --abort", verbose=True)
        raise RuntimeError(f"failed to revert the revert commit {revert_commit}")
    if not Git.push(repo, f"HEAD:refs/heads/{branch}", force=True):
        raise RuntimeError(f"failed to push {branch}")
    return create_pull_request(
        repo,
        branch,
        f'Reapply "{pull_request["title"]}"',
        reintroduce_body(pull_request, revert_pull_request, investigation),
        draft=True,
    )


def investigate(failure: Failure, index: int) -> Investigation:
    """Ask the agent about one failure and return the recorded investigation."""
    investigation = Investigation(failure=failure)
    verdict_file = os.path.abspath(f"{TEMP_DIR}/investigation_{index}.json")
    prompt = investigation_prompt(failure, verdict_file)
    raw = ""
    error = ""
    for attempt in range(1, MAX_AGENT_ATTEMPTS + 1):
        # The agent runs long and makes GitHub calls throughout; refresh the App
        # token before each attempt so that it cannot expire midway.
        GHAuth.auth(no_strict=True)
        reset_worktree()
        try:
            raw = run_agent(prompt, verdict_file)
            for name, value in parse_verdict(raw).items():
                setattr(investigation, name, value)
            error = ""
            break
        except Exception as e:  # noqa: BLE001 -- any failure here is worth one retry
            error = f"{type(e).__name__}: {e}"
            print(
                f"WARNING: investigation attempt {attempt}/{MAX_AGENT_ATTEMPTS} of "
                f"{failure.title!r} failed: {error}"
            )
            traceback.print_exc()
        finally:
            reset_worktree()
    if error:
        investigation.verdict = "error"
        investigation.confidence = ""
        investigation.explanation = (
            f"The investigation failed: {error}. Agent output: {raw[:2000]}"
        )
    return investigation


def act(investigation: Investigation, repo: str, now: datetime) -> None:
    """Revert the pull request the investigation blames, if it may be reverted.

    Updates the investigation in place with what happened. Anything that stops
    the revert -- a guard, a conflict, a revert that already exists -- is a
    recorded outcome, not a failure of the job. Only an unexpected error during a
    revert that has already started is raised, because leaving that unnoticed
    would be worse than a red job.
    """
    number = investigation.offending_pull_request_number
    pull_request = get_pull_request(number, repo)
    if not pull_request:
        investigation.action = Action.SKIPPED_GUARD
        investigation.note(f"Not reverted: pull request #{number} could not be read.")
        return

    guard = culprit_guard(pull_request, investigation.failure, now)
    if guard:
        investigation.action = Action.SKIPPED_GUARD
        investigation.note(f"Not reverted: {guard}.")
        print(f"Not reverting #{number}: {guard}")
        return

    merge_commit = pull_request["mergeCommit"]["oid"]
    if not Shell.check(
        f"git merge-base --is-ancestor {shlex.quote(merge_commit)} origin/{BASE_BRANCH}"
    ):
        investigation.action = Action.SKIPPED_GUARD
        investigation.note(
            f"Not reverted: the merge commit {merge_commit} of pull request #{number} "
            f"is not in the history of {BASE_BRANCH}."
        )
        return

    branch = f"{REVERT_BRANCH_PREFIX}{number}"
    handled = already_handled(merge_commit, branch, repo)
    if handled:
        investigation.action = (
            Action.ALREADY_REVERTED
            if "already on" in handled
            else Action.REVERT_IN_FLIGHT
        )
        investigation.note(f"Not reverted: {handled}.")
        print(f"Not reverting #{number}: {handled}")
        return

    try:
        revert_commit, revert_pull_request = create_revert(
            pull_request, merge_commit, investigation, repo
        )
    except RevertConflict as e:
        investigation.action = Action.REVERT_CONFLICT
        investigation.note(f"Not reverted: {e}.")
        print(f"Not reverting #{number}: {e}")
        return
    except (
        Exception
    ) as e:  # noqa: BLE001 -- recorded first, then re-raised to fail the job
        investigation.action = Action.REVERT_FAILED
        investigation.note(f"The revert failed: {type(e).__name__}: {e}.")
        raise

    investigation.action = Action.REVERTED
    investigation.revert_pull_request_number = revert_pull_request
    print(f"Reverted #{number} in #{revert_pull_request} and merged it")

    try:
        investigation.reintroduce_pull_request_number = create_reintroduce(
            pull_request, revert_commit, revert_pull_request, investigation, repo
        )
    except Exception as e:  # noqa: BLE001 -- recorded first, then re-raised
        # The revert is already merged, so the change is off master with nothing
        # holding it. Say so in the row and fail the job: somebody has to reopen
        # it by hand.
        investigation.note(
            f"The revert is merged, but reintroducing the change failed: "
            f"{type(e).__name__}: {e}. Pull request #{number} has to be reopened "
            f"by hand."
        )
        raise
    print(
        f"Opened #{investigation.reintroduce_pull_request_number} "
        f"to reintroduce #{number}"
    )


def prepare(info: Info) -> bool:
    """Refuse to run anywhere but on the current tip of the base branch, then
    get the checkout ready.

    The job is scheduled, but GitHub also allows dispatching it and replaying old
    runs, and this one merges pull requests with administrator privileges: that
    must never happen from a feature branch. Full history is fetched because
    reverting a merge needs the commit and its parents, which a shallow checkout
    does not have."""
    if info.git_branch != BASE_BRANCH:
        print(
            f"Refusing to run: this job reverts and merges pull requests, so it must "
            f"run against {BASE_BRANCH!r}, not {info.git_branch!r}.",
            file=sys.stderr,
        )
        return False
    Shell.check(
        "git rev-parse --is-shallow-repository | grep -q true && "
        "git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 "
        f"origin {BASE_BRANCH} ||:",
        verbose=True,
    )
    Shell.check(
        f"git fetch --no-tags --prune --no-recurse-submodules origin "
        f"+refs/heads/{BASE_BRANCH}:refs/remotes/origin/{BASE_BRANCH}",
        verbose=True,
        strict=True,
    )
    Shell.check(
        f"git config user.name {shlex.quote(ROBOT_NAME)} && "
        f"git config user.email {shlex.quote(ROBOT_EMAIL)}",
        verbose=True,
        strict=True,
    )
    reset_worktree()
    return True


def connect() -> CIDB:
    info = Info()
    url, user, password = (
        info.get_secret(Settings.SECRET_CI_DB_URL)
        .join_with(info.get_secret(Settings.SECRET_CI_DB_USER))
        .join_with(info.get_secret(Settings.SECRET_CI_DB_PASSWORD))
        .get_value()
    )
    return CIDB(url=url, user=user, passwd=password)


def select_failures(cidb: CIDB, now: datetime) -> List[Failure]:
    """The failures to investigate in this run, most frequent first."""
    failures = [
        Failure.from_row(row)
        for row in parse_json_each_row(cidb.query(failures_query()))
    ]
    print(
        f"{len(failures)} failures repeated more than {MIN_FAILURES - 1} times "
        f"on {BASE_BRANCH} in the last {FAILURE_WINDOW_HOURS} hours"
    )
    prior = {
        (row["failure_kind"], row["test_name"], row["check_name"]): row
        for row in parse_json_each_row(cidb.query(recent_investigations_query()))
    }
    selected = []
    for failure in failures:
        reason = skip_reason(failure, prior, now)
        if reason:
            print(f"  skipping {failure.title!r}: {reason}")
            continue
        selected.append(failure)
    if len(selected) > MAX_INVESTIGATIONS_PER_RUN:
        print(
            f"  {len(selected)} failures to investigate, taking the "
            f"{MAX_INVESTIGATIONS_PER_RUN} most frequent ones; the next run picks up the rest"
        )
        selected = selected[:MAX_INVESTIGATIONS_PER_RUN]
    for failure in selected:
        print(f"  investigating {failure.title!r} ({failure.failure_count} failures)")
    return selected


def record(cidb: CIDB, investigations: List[Investigation], now: datetime) -> None:
    """Write every investigation, the negative ones included, to the CI database."""
    if not investigations:
        print("Nothing to record")
        return
    task_url = Info().get_job_url()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    cidb.insert_rows(
        [json.dumps(i.to_record(timestamp, task_url)) for i in investigations],
        table=INVESTIGATION_TABLE,
    )


def summary(investigations: List[Investigation]) -> str:
    if not investigations:
        return "No failures to investigate"
    parts = [
        f"{len(investigations)} investigated",
        f"{sum(i.verdict == 'regression' for i in investigations)} regressions",
        f"{sum(i.action == Action.REVERTED for i in investigations)} reverted",
    ]
    errors = sum(i.verdict == "error" for i in investigations)
    if errors:
        parts.append(f"{errors} failed to investigate")
    return ", ".join(parts)


def step(results: List[Result], name: str, command) -> bool:
    """Run one stage of the job as a praktika sub-result and report whether it
    succeeded, so the caller can stop at the first broken stage."""
    results.append(Result.from_commands_run(name=name, command=command))
    return results[-1].is_ok()


def run(results: List[Result]) -> None:
    info = Info()
    os.makedirs(TEMP_DIR, exist_ok=True)

    if not step(results, f"Run against current {BASE_BRANCH}", lambda: prepare(info)):
        return

    cidb = connect()
    if not step(
        results,
        f"Create {INVESTIGATION_TABLE}",
        lambda: cidb.query(INVESTIGATION_TABLE_DDL) is not None,
    ):
        return

    now = datetime.now(timezone.utc)
    failures: List[Failure] = []
    if not step(
        results,
        "Select repeated failures",
        lambda: bool(failures.extend(select_failures(cidb, now)) or True),
    ):
        return

    investigations: List[Investigation] = []
    reverts = 0
    try:
        for index, failure in enumerate(failures):
            spent = datetime.now(timezone.utc) - now
            if spent.total_seconds() > RUN_BUDGET_SEC:
                print(
                    f"Out of time after {int(spent.total_seconds() // 60)} minutes; "
                    f"{len(failures) - index} failures are left to the next run"
                )
                break
            investigation = investigate(failure, index)
            investigations.append(investigation)
            print(
                f"{failure.title!r}: {investigation.verdict} "
                f"({investigation.confidence or 'no'} confidence)"
            )
            if not investigation.is_actionable():
                continue
            if reverts >= MAX_REVERTS_PER_RUN:
                investigation.action = Action.SKIPPED_LIMIT
                investigation.note(
                    f"Not reverted: this run already made {MAX_REVERTS_PER_RUN} reverts, "
                    f"which is the limit; the next run picks this up."
                )
                continue
            step(
                results,
                f"Revert #{investigation.offending_pull_request_number}",
                lambda i=investigation: act(i, info.repo_name, now) or True,
            )
            if investigation.action == Action.REVERTED:
                reverts += 1
            reset_worktree()
            if investigation.action == Action.REVERT_FAILED:
                # A revert that got as far as failing means something outside
                # this job's judgement is wrong -- a push that was refused, a
                # merge that was not permitted. Trying the next failure would
                # only leave more half-finished reverts behind.
                print(
                    "Stopping after a failed revert; the rest is left to the next run"
                )
                break
    finally:
        # Record what was investigated even if a revert blew up halfway: these
        # rows are how anybody finds out what this job did.
        step(
            results,
            "Record investigations",
            lambda: record(cidb, investigations, now) or True,
        )
        results[-1].set_info(summary(investigations))


if __name__ == "__main__":
    job_results: List[Result] = []
    try:
        run(job_results)
    except Exception as error:
        print(f"ERROR: {error}")
        traceback.print_exc()
        job_results.append(
            Result(
                name="Revert CI regressions",
                status=Result.Status.FAIL,
                info=f"ERROR: {error}",
            )
        )
    Result.create_from(results=job_results).complete_job()
