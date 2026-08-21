"""Guards for the auto-retry's "has never been retried" selector.

``.github/workflows/retry_infra_failures.yml`` retries infra-killed PR runs hourly.
A fork PR's approval gate concludes ``run_attempt`` 1 as ``action_required`` with zero
jobs, so its first attempt that actually runs anything is 2, and a selector keyed on
``attempt == 1`` can never see it. The invariant is "exactly one attempt executed":
attempt 1 for a same-repo run, attempt 2 behind an approval gate.

The step is read as text (like ``test_create_release.py``) so no YAML parser is needed.
The listing filter's coverage is static (its three ``jq`` stages are pinned by exact
equality); the ``jq``-gated behavioural rows cover eligibility and rerun. ``jq`` is absent
from the CI Tests image, so in CI only the static assertions execute and they are what
must pin the step's semantics.
"""

import datetime
import json
import os
import re
import shutil
import stat
import subprocess
import textwrap

import pytest

WORKFLOW = os.path.join(
    os.path.dirname(__file__), "../../.github/workflows/retry_infra_failures.yml"
)

RUN_ID = 42

# The probe, normalized (line continuations and runs of whitespace collapsed). Pinning the
# whole command at once covers the run identifier, the attempt read, the field, and the
# fail-closed fallback: a substring assertion on any one of them leaves the others free.
PROBE_CMD = (
    'attempt1_conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run_id/attempts/1" '
    "--jq '.conclusion' 2>/dev/null || echo \"\")"
)
REJECT_COND = 'if [ "$attempt1_conclusion" != "action_required" ]; then'

# Every command of the candidate's data path, from the listing to the reject condition,
# normalized. A census of ASSIGNMENTS has to enumerate the separators one can follow, and
# that enumeration keeps leaking: `&&`, `||`, `&`, a `{ ... }` group, `read -r var`,
# `printf -v var` and `declare "${v}=2"` all introduce one without a separator the census
# looks for, and each forges run_id, run_attempt or attempt1_conclusion -- either removing
# the never-retried invariant or making the gate inert. A closed whitelist of the region's
# commands refuses every such form, including the ones no regex can name.
# The listing command is matched as a PATTERN: its semantics are pinned by the stage-equality
# assert and by the `--json ... attempt` assert above, so re-pinning its flag spelling here
# only refuses harmless edits (`--limit`, field order, flag order).
LISTING_RE = re.compile(r"^if page=\$\(gh run list .*\); then$")
# One simple `echo`: no separator (`;` `&` `|`), no grouping or substitution. Such a line
# cannot assign to the shell, so dropping it admits a log line without admitting an
# assignment. Narrowing this pattern can only cause a false refusal, never a false accept.
ECHO_RE = re.compile(r"^echo (?:[^;&|(){}`]*)$")
EXPECTED_DATA_PATH = [
    'run_entries=""',
    "hour=0",
    'while [ "$hour" -lt $((LOOKBACK_DAYS * 24)) ]; do',
    'from=$(date -u -d "$((hour + PARTITION_HOURS)) hours ago" +%Y-%m-%dT%H:%M:%SZ)',
    'to=$(date -u -d "$hour hours ago" +%Y-%m-%dT%H:%M:%SZ)',
    'page=""',
    "for attempt_no in 1 2 3; do",
    "<LISTING>",
    "break",
    "fi",
    'page=""',
    "sleep $((attempt_no * 5))",
    "done",
    'if [ -z "$page" ]; then',
    "exit 1",
    "fi",
    "page_rows=$(echo \"$page\" | jq 'length')",
    'if [ "$page_rows" -ge "$PAGE_LIMIT" ]; then',
    "exit 1",
    "fi",
    'matches=$(echo "$page" | jq -r ".[] | select(.attempt <= 2 and .startedAt >= '
    '\\"$cutoff\\") | \\"\\(.startedAt) \\(.databaseId):\\(.attempt)\\"")',
    'if [ -n "$matches" ]; then',
    "run_entries=$(printf '%s\\n%s' \"$run_entries\" \"$matches\")",
    "fi",
    "hour=$((hour + PARTITION_HOURS))",
    "done",
    "run_entries=$(echo \"$run_entries\" | grep -v '^$' | sort -u | awk '{print $2}' "
    "|| true)",
    'if [ -z "$run_entries" ]; then',
    "exit 0",
    "fi",
    "for run_entry in $run_entries; do",
    'if [ "$rerun_count" -ge "$MAX_RERUNS" ]; then',
    "break",
    "fi",
    'run_id="${run_entry%%:*}"',
    'run_attempt="${run_entry##*:}"',
    'if [ "$run_attempt" != "1" ]; then',
    'attempt1_conclusion=$(gh api "repos/$GH_REPO/actions/runs/$run_id/attempts/1" '
    "--jq '.conclusion' 2>/dev/null || echo \"\")",
    'if [ "$attempt1_conclusion" != "action_required" ]; then',
]

GH_STUB = """#!/usr/bin/env bash
# The caller's own --jq expression, so the stub answers the field that was asked for rather
# than a fixed string: requesting anything but .conclusion must be observable.
_jq_expr() {
    while [ $# -gt 0 ]; do
        if [ "$1" = "--jq" ]; then echo "$2"; return; fi
        shift
    done
}
# Likewise the --json field list, so the stub answers only the fields that were requested:
# dropping one from the invocation must be observable rather than masked by the fixture.
_json_fields() {
    while [ $# -gt 0 ]; do
        if [ "$1" = "--json" ]; then echo "$2"; return; fi
        shift
    done
}
if [ "$1" = "run" ] && [ "$2" = "list" ]; then
    # The step walks createdAt partitions, so this is called once per partition. The fixture
    # answers the FIRST partition only and every later one is empty: a duplicate answer would
    # hide a missing dedup, and answering only the last would hide a missing accumulation.
    _expr=$(_jq_expr "$@")
    if [ -f "$LISTED_MARKER" ]; then
        _out='[]'
    else
        : > "$LISTED_MARKER"
        # Project the fixture onto the requested fields, like the real command: an omitted
        # field must come back absent, not be supplied anyway by the fixture.
        _out=$(_json_fields "$@" | tr ',' '\n' | jq -R . | jq -s \
            --slurpfile rows "$FIXTURE" '. as $f | $rows[0] | map(with_entries(
                select(.key as $k | $f | index($k))))')
    fi
    # Answer whatever was asked for: with --jq the caller wants the projection, without it
    # the raw page (which the walk then counts and filters itself).
    if [ -n "$_expr" ]; then echo "$_out" | jq -r "$_expr"; else echo "$_out"; fi
    exit 0
fi
if [ "$1" = "run" ] && [ "$2" = "rerun" ]; then
    # Record the argv so a test can assert the rerun really happened.
    echo "$*" >> "$RERUN_LOG"
    exit 0
fi
case "$*" in
    *attempts/1*)
        if [ -n "${ATTEMPT1_FAIL:-}" ]; then
            echo "stub: simulated attempt-1 API error" >&2
            exit 1
        fi
        echo "$ATTEMPT1_JSON" | jq -r "$(_jq_expr "$@")"
        ;;
    *'/jobs?'*) echo "$JOBS_JSON" ;;
    *) echo '{}' ;;
esac
"""

NO_JOBS = '{"jobs": []}'
# A failed "Config Workflow" job: the workflow's unchanged job-level heuristic treats
# that name as infrastructure outright, so no timestamp arithmetic is needed.
INFRA_JOBS = '{"jobs": [{"name": "Config Workflow", "conclusion": "failure", "steps": []}]}'


def _retry_step():
    """The shell body of the ``retry-failed`` job's only step, read as text so that no
    YAML parser is needed (``test_create_release.py`` reads its workflow the same way)."""
    with open(WORKFLOW, encoding="utf-8") as f:
        lines = f.read().splitlines()
    start = next(i for i, l in enumerate(lines) if l.strip() == "run: |") + 1
    indent = len(lines[start]) - len(lines[start].lstrip())
    body = []
    for line in lines[start:]:
        if line.strip() and len(line) - len(line.lstrip()) < indent:
            break
        body.append(line)
    step = textwrap.dedent("\n".join(body))
    # A silently empty extraction would make every assertion below vacuous.
    assert "MAX_RERUNS" in step and "gh run list" in step, step
    return step


def _listing_jq(step):
    """The candidate filter's jq expression and the listing invocation it feeds.

    Both are read from a NORMALIZED form (continuations joined, whitespace runs collapsed) so
    a re-wrap is not a false refusal. The listing fetches a createdAt partition as JSON and a
    separate ``jq`` selects the candidates from it, so the two are found independently.
    Returns (expr, invocation).
    """
    assert "gh run list" in step, step
    invocation = " ".join(
        step.split("gh run list", 1)[1].split("\n\n", 1)[0].replace("\\\n", " ").split()
    )
    # The one jq that turns a fetched page into `id:attempt` candidates. Anchored on the
    # assignment so an unrelated jq elsewhere in the step is never mistaken for it, and
    # matched line-wise (continuations joined first) so `.*` cannot run past the expression
    # into the rest of the step: it contains `\"` sequences, so neither a greedy nor a
    # non-greedy match against the whole step ends at the right place.
    joined = step.replace("\\\n", " ")
    m = next(
        (
            m
            for line in joined.splitlines()
            if (
                m := re.search(
                    r'matches=\$\(echo "\$page" \| jq -r "(.*)"\)\s*$', " ".join(line.split())
                )
            )
        ),
        None,
    )
    assert m, joined
    return m.group(1), invocation


def _jq_stages(expr):
    """``expr`` split on top-level ``|``: parenthesised groups and ``\\"``-quoted jq
    strings do not separate stages."""
    stages, cur, depth, i, in_str = [], "", 0, 0, False
    while i < len(expr):
        if expr[i : i + 2] == '\\"':
            in_str = not in_str
            cur += expr[i : i + 2]
            i += 2
            continue
        c = expr[i]
        if not in_str:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif c == "|" and depth == 0:
                stages.append(cur.strip())
                cur = ""
                i += 1
                continue
        cur += c
        i += 1
    stages.append(cur.strip())
    return stages


_CONTINUE = re.compile(r"^continue\b(?!=)\s*[0-9]*\s*(?:;\s*:\s*)?$")


def _reject_branch_body(step):
    """The significant lines of the already-retried branch's body, comments dropped."""
    lines = step.splitlines()
    idx = next(i for i, l in enumerate(lines) if '!= "action_required" ]; then' in l)
    indent = len(lines[idx]) - len(lines[idx].lstrip())
    body = []
    for l in lines[idx + 1 :]:
        if l.strip() and len(l) - len(l.lstrip()) <= indent:
            break  # the branch's own `fi`
        s = l.strip()
        if s and not s.startswith("#"):
            body.append(s)
    return body


def _data_path_commands(step):
    """The significant commands of the candidate's data path, comments dropped.

    The region runs from the ``run_entries`` initializer to the reject condition inclusive:
    starting at the loop header instead would leave the createdAt walk and the empty-listing
    guard outside it, where an assignment can neutralize the selector unseen. Continuations are
    joined and whitespace runs collapsed BEFORE splitting, so a re-wrap is not a refusal.
    """
    lines = step.splitlines()
    start = next(i for i, l in enumerate(lines) if l.strip() == 'run_entries=""')
    end = next(i for i, l in enumerate(lines) if '!= "action_required" ]; then' in l)
    assert start < end, (start, end)
    joined = "\n".join(lines[start : end + 1]).replace("\\\n", " ")
    body = []
    for l in joined.splitlines():
        s = " ".join(l.split())
        if not s or s.startswith("#"):
            continue
        if ECHO_RE.match(s):
            continue
        body.append("<LISTING>" if LISTING_RE.match(s) else s)
    return body


def test_selector_admits_gated_fork_runs():
    step = _retry_step()
    # The probe spans continuation lines, so its assertions below read this NORMALIZED
    # form (continuations joined, whitespace runs collapsed). A raw-substring form
    # refuses a harmless re-wrap while the probe still reads .conclusion and still
    # fails closed.
    normalized = " ".join(step.replace("\\\n", " ").split())
    listing_jq, listing_invocation = _listing_jq(step)
    # Pin all THREE stages exactly. A shape-only check (three stages, stage 2 starts with
    # `select(`) leaves the projection free, and a filter nested INSIDE it -- `("\(.databaseId):
    # \(.attempt)" | select(false))` -- keeps the stage count, the stage-1 prefix and the
    # projection substring all intact while jq emits nothing at all, so the listing yields no
    # candidates and the whole fix is inert.
    assert _jq_stages(listing_jq) == [
        ".[]",
        r"select(.attempt <= 2 and .startedAt >= \"$cutoff\")",
        r"\"\(.startedAt) \(.databaseId):\(.attempt)\"",
    ], f"the listing filter must be exactly these three stages: {_jq_stages(listing_jq)}"
    # `.attempt` steers the guard, so the field must be requested. Without it jq yields
    # null, `null <= 2` still admits the row, and run_attempt becomes the string "null":
    # a same-repo attempt-1 run is then skipped and a fork run aborts the step at
    # `$((run_attempt + 1))`.
    m = re.search(r"--json\s+([A-Za-z0-9_,]+)", listing_invocation)
    fields = m.group(1).split(",") if m else []
    assert m and "attempt" in fields, listing_invocation
    # The window field must be requested too: without it jq compares null against the
    # cutoff, `null >= "..."` is false, and the listing silently yields nothing.
    assert "startedAt" in fields, listing_invocation
    # And the run identifier, which the projection reads: without it every candidate is
    # `null:<attempt>`, so the probe queries run `null` and no run is ever retried.
    assert "databaseId" in fields, listing_invocation
    # The list is ordered by createdAt descending while the window keys on startedAt, so a
    # gated run's page position is set by its OLD createdAt, not by the window it belongs to.
    # Neither a row cap nor a fixed createdAt bound can therefore exhaust the window: how deep
    # the oldest in-window candidate sits is set by failure VOLUME, and how far back it was
    # created is set by a gate delay reaching 26.6 days. So the scan must WALK createdAt in
    # partitions, over a lookback covering the 30 day approval expiry.
    assert re.search(r'--created\s+"\$from\.\.\$to"', listing_invocation), (
        f"the scan must walk createdAt partitions, not read one fixed page: {listing_invocation}"
    )
    # Both partition endpoints must be TIMESTAMPS. Both endpoints of a bare-date range are
    # inclusive whole days, so `--created "$from..$to"` with `+%Y-%m-%d` ignores the partition
    # width entirely: a 24h partition then returns two whole days (measured 927 rows against
    # 480 and 447 individually), which can saturate the ceiling with no width left to reduce.
    for var in ("from", "to"):
        assert re.search(
            r"^\s*" + var + r"=\$\(date -u -d \"[^\"]*\" \+%Y-%m-%dT%H:%M:%SZ\)\s*$", step, re.M
        ), f"{var} must be a timestamp, not a bare date"
    # The walk is only as good as its constants: a 2-day LOOKBACK_DAYS leaves the blind spot
    # open while every flag-shaped assertion above still passes.
    decl = re.search(r"^\s*LOOKBACK_DAYS=([0-9]+)\s*$", step, re.M)
    assert decl and int(decl.group(1)) >= 31, decl.group(0) if decl else None
    # PAGE_LIMIT is pinned EXACTLY to the API's ceiling, not as a lower bound: the cap is the
    # API's, so raising this cannot fetch more rows, it only stops `page_rows >= PAGE_LIMIT`
    # from ever being true and silently disables the truncation check.
    decl = re.search(r"^\s*PAGE_LIMIT=([0-9]+)\s*$", step, re.M)
    assert decl and int(decl.group(1)) == 1000, decl.group(0) if decl else None
    # The fetch must ASK for PAGE_LIMIT rows. A literal `--limit 1` leaves the saturation
    # check comparing 1 against 1000, so it never fires while each partition returns one row.
    assert re.search(r'--limit\s+"\$PAGE_LIMIT"', listing_invocation), listing_invocation
    # A partition wide enough to saturate the ceiling is truncated, which is the very blind
    # spot this walk closes. The busiest single day observed holds ~480 failed runs.
    decl = re.search(r"^\s*PARTITION_HOURS=([0-9]+)\s*$", step, re.M)
    assert decl and 0 < int(decl.group(1)) <= 24, decl.group(0) if decl else None
    # Truncation must be loud. Without this the walk still scans 31 partitions and still
    # reports success while dropping every candidate past row 1000 of a saturated day.
    assert re.search(r'"\$page_rows"\s+-ge\s+"\$PAGE_LIMIT"', step) and re.search(
        r"page_rows.*PAGE_LIMIT.*\n(?:.*\n)*?\s*exit 1$", step, re.M
    ), "a saturated partition must fail the step, not be scanned past silently"
    # The walk must accumulate: a body that overwrites `run_entries` each partition keeps the
    # loop, the bound and the flags intact while only the last day survives.
    assert re.search(r"run_entries=\$\(printf\s+'%s\\n%s'\s+\"\$run_entries\"", step), (
        "each partition's matches must be appended to run_entries, not replace them"
    )
    # Adjacent partitions share a date boundary, so one run can be listed twice and consume
    # two of MAX_RERUNS.
    dedup = re.search(r"^\s*run_entries=\$\(echo \"\$run_entries\".*$", step, re.M)
    assert dedup and "sort -u" in dedup.group(0), (
        "the accumulated candidates must be deduplicated",
        dedup.group(0) if dedup else None,
    )
    # MAX_RERUNS bites this list, so its ORDER decides which candidates are reached. A run is
    # admitted by a 6 hour startedAt window, so the one closest to leaving it must be reached
    # first: it is retried on this tick or never, while a fresher one still has later ticks.
    # Sorting must therefore key on startedAt, not the databaseId that leads the entry -- id
    # order is only a 0.90 pairwise proxy for it, and either id direction loses more last-tick
    # candidates (measured over 312 hourly clocks: 35% here, 56% ascending, 100% descending).
    assert re.search(r"\\\"\\\(\.startedAt\) ", listing_jq), (
        "each entry must lead with startedAt so the dedup sort orders by deadline",
        listing_jq,
    )
    # And the key must be stripped again, or run_id parses out of the timestamp.
    assert "awk '{print $2}'" in dedup.group(0), (
        "the sort key must be dropped once the order is fixed", dedup.group(0)
    )
    # The `<LISTING>` sentinel intentionally does not pin the invocation's flag SPELLING, but
    # the three flags below are semantics, not spelling: `--status success`, another
    # `--workflow`, or another `--repo` all leave the whole static test green while the job
    # retries the wrong runs. Asserted against the normalized invocation, so a re-wrap or a
    # flag reorder is not a false refusal. Matched token-bounded because substring
    # membership is satisfied by any EXTENSION of the value: `pull_request.yml.bak` and
    # `failurefoo` repoint the query while containing the expected text. The invocation is
    # whitespace-normalized, so `\s` is an exact boundary.
    for flag in ('--repo "$GH_REPO"', "--workflow pull_request.yml", "--status failure"):
        assert re.search(
            r"(?:^|\s)" + re.escape(flag) + r"(?=\s|$)", listing_invocation
        ), (flag, listing_invocation)
    assert 'attempt1_conclusion" != "action_required"' in step, "missing attempt-1 probe"
    # The probe must interrogate attempt 1: reading any other attempt makes it meaningless.
    # Against `normalized` (hoisted above), so a re-wrap is not a false refusal.
    assert "/attempts/1" in normalized, "the probe must read attempt 1"
    # It must be reached only for a candidate that is NOT attempt 1.
    GUARD = 'if [ "$run_attempt" != "1" ]; then'
    assert GUARD in step, "probe must guard on attempt != 1"
    # Text presence is not reachability: wrapping the whole probe block in `if false; then`
    # keeps every assertion above true while the never-retried invariant stops holding.
    # So pin that the guard IS the condition gating the probe -- loop-body level, with no
    # enclosing conditional still open.
    lines = step.splitlines()
    loop = next(i for i, l in enumerate(lines) if l.strip().startswith("for run_entry in"))
    guard = next(i for i, l in enumerate(lines) if GUARD in l)
    assert loop < guard, (loop, guard)
    depth = 0
    for l in lines[loop + 1 : guard]:
        s = l.strip()
        if re.match(r"^(if|while|until)\b", s):
            depth += 1
        elif s in ("fi", "done"):
            depth -= 1
    assert depth == 0, f"probe is nested inside an enclosing conditional (depth {depth})"
    body_indent = min(
        len(l) - len(l.lstrip()) for l in lines[loop + 1 : guard] if l.strip()
    )
    assert len(lines[guard]) - len(lines[guard].lstrip()) == body_indent, (
        "the probe's guard must sit at loop-body level, not nested one level deeper"
    )
    # The scan above spans only the lines BEFORE the guard, so it cannot see a conditional
    # opened INSIDE it: wrapping the probe in `if false; then` immediately after the guard
    # line keeps every assertion so far true while the probe never runs. So pin that the
    # probe IS the guard's body -- its first significant line, at depth 0.
    guard_indent = len(lines[guard]) - len(lines[guard].lstrip())
    guard_body = []
    for l in lines[guard + 1 :]:
        if l.strip() and len(l) - len(l.lstrip()) <= guard_indent:
            break  # the guard's own matching `fi`
        guard_body.append(l)
    significant = [
        l.strip() for l in guard_body if l.strip() and not l.strip().startswith("#")
    ]
    body_depth = 0
    for s in significant:
        if '!= "action_required" ]; then' in s:
            break
        if re.match(r"^(if|while|until|case)\b", s):
            body_depth += 1
        elif s in ("fi", "done", "esac"):
            body_depth -= 1
    assert (
        significant
        and significant[0].startswith("attempt1_conclusion=$(gh api")
        and body_depth == 0
    ), "the attempt-1 probe must BE the guard's body, not sit behind another conditional"
    # The branch must SKIP the run, not just log. Depth-tracking a `continue` has to
    # enumerate every construct that can nest one, and the enumeration keeps leaking: a
    # `{ if false; then` / `fi; }` block reads as top-level because the brace opens no
    # tracked depth while `fi; }` closes one. Whitelist the body instead -- log lines, then
    # a `continue` as the last command. `(?!=)` still rejects `continue=false`, an
    # assignment, and `continue 1` / `continue ; :` stay accepted, both being equivalent.
    reject_body = _reject_branch_body(step)
    assert reject_body and _CONTINUE.match(reject_body[-1]), (
        "the reject branch must end in a top-level `continue`, or the gate is inert",
        reject_body,
    )
    assert all(
        l.startswith("echo ") for l in reject_body[:-1]
    ), f"the reject branch must only log before it continues: {reject_body}"
    # The projection (pinned as stage 3 above) and the two splits must agree on field order:
    # transposing either makes run_id and run_attempt swap, so the probe reads a nonexistent
    # run and rejects every gated candidate while every assert above still passes.
    assert 'run_id="${run_entry%%:*}"' in step, "run id must come from the head field"
    assert 'run_attempt="${run_entry##*:}"' in step, "attempt must come from the tail field"
    # The candidate's data path, whitelisted whole. Censusing the ASSIGNMENTS in it has to
    # enumerate the separators one can follow, and that enumeration leaks the same way the
    # `continue` depth-tracking above did: an assignment after `&&`, `||` or `&`, inside a
    # `{ ... }` group, or via `read`/`printf -v`/`declare` carries no separator the census
    # looks for. Forging run_attempt (to 1) makes EVERY attempt-2 candidate eligible, so the
    # never-retried contract this PR adds goes unpinned; forging run_id or
    # attempt1_conclusion makes the gate inert instead. So pin the region's commands.
    assert _data_path_commands(step) == EXPECTED_DATA_PATH, (
        "the candidate's data path must be exactly EXPECTED_DATA_PATH",
        _data_path_commands(step),
    )
    # A gated attempt 1 has status "completed" and conclusion "action_required", so a probe
    # reading any other field rejects every fork candidate while the asserts above still pass.
    assert (
        "--jq '.conclusion'" in normalized
    ), "the probe must read the conclusion field"
    # Without the fallback a transient API error aborts the step under "set -euo pipefail",
    # taking down the whole hourly retry rather than skipping one run.
    assert (
        '2>/dev/null || echo ""' in normalized
    ), "the probe must fail closed on an API error"
    # The assertions above pin the probe's PARTS but not the whole: swapping the run
    # identifier ($run_id -> $run_attempt) keeps "/attempts/1" present while the probe reads
    # a nonexistent run (404 -> empty -> every gated fork candidate skipped), and extending
    # the reject condition with an always-true disjunct keeps its substring present while
    # skipping every attempt-2 candidate. Both leave the fix inert with CI green.
    assert (
        PROBE_CMD in normalized
    ), "the attempt-1 probe command must be exactly PROBE_CMD"
    # ...and it must be the SOLE assignment: appending `attempt1_conclusion="failure"`
    # after the correct command satisfies the substring check above while the reject
    # branch then skips every attempt-2 candidate.
    assigns = re.findall(r"^\s*attempt1_conclusion=(.*)$", step, re.M)
    assert len(assigns) == 1, assigns
    # ...and it must be the SOLE test of the probe's result: an added disjunct would
    # otherwise satisfy the equality above while making the branch unconditional.
    conds = [
        l.strip()
        for l in step.splitlines()
        if "$attempt1_conclusion" in l and l.strip().startswith("if ")
    ]
    assert conds == [REJECT_COND], conds
    # The asserts above pin the gate and everything up to the reject condition, but the region
    # from the gate's own `fi` to `gh run rerun` is unconstrained: re-emitting the guard there
    # with a bare `continue`, or forcing `should_rerun=false` for an attempt-2 candidate just
    # before the rerun, skips every gated fork run while every assertion above still passes.
    # So pin that exactly ONE line branches on run_attempt -- the gate -- and that run_attempt
    # is assigned exactly once. Other reads (the rerun log line) are free.
    attempt_reads = [
        s
        for s in (" ".join(l.split()) for l in step.splitlines())
        if s
        and not s.startswith("#")
        and not re.match(r"^run_attempt=", s)
        and re.search(r"\brun_attempt\b", s)
    ]
    # The keywords fire at any COMMAND position -- line start or after a separator or
    # grouping character -- because anchoring them at line start alone misses a branch
    # introduced after one: `echo x; if [ "$run_attempt" ...` read as non-branching, as did
    # a `{ ... }` group, a `( ... )` subshell and `: ; case ...`. `[` is included: a `test`
    # reading $run_attempt is a branch in every practical form.
    branching = [
        s
        for s in attempt_reads
        if re.search(
            r"(?:^|[;&|(){}]\s*)"
            r"(if|while|until|case|elif|then|else|continue|break|return)\b"
            r"|&&|\|\||\bcontinue\b|\bbreak\b|\breturn\b|\[",
            s,
        )
    ]
    assert branching == [GUARD], (
        "exactly one line may branch on run_attempt (the gate); a second one makes it inert",
        branching,
    )
    assert (
        len(re.findall(r"^\s*run_attempt=", step, re.M)) == 1
    ), "run_attempt must be assigned once"


def _stamp(hours):
    """An ISO-8601 stamp ``hours`` in the past, in the format the API returns."""
    return (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_step(
    tmp_path,
    attempt,
    attempt1_conclusion,
    age_hours,
    jobs_json,
    attempt1_fail=False,
    started_age_hours=None,
):
    """Run the real step over a one-row fixture. Returns (proc, rerun_log_text).

    ``attempt1_conclusion`` becomes the ``conclusion`` of a full attempt payload whose
    ``status`` is always ``completed`` (what the real API returns for a gated attempt), so a
    probe reading the wrong field gets ``completed`` rather than the expected value.

    ``age_hours`` ages ``createdAt`` and ``started_age_hours`` ages ``startedAt``; both are
    emitted always, so a predicate reading either field finds it. They default to equal,
    which is what the API returns for an ungated run. Passing a SMALLER
    ``started_age_hours`` models the approval gate: ``createdAt`` stays pinned to attempt 1
    while the attempt that actually ran started later.
    """
    created_at = _stamp(age_hours)
    started_at = _stamp(age_hours if started_age_hours is None else started_age_hours)
    fixture = tmp_path / "runs.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "databaseId": RUN_ID,
                    "attempt": attempt,
                    "createdAt": created_at,
                    "startedAt": started_at,
                }
            ]
        )
    )
    rerun_log = tmp_path / "reruns.log"
    rerun_log.write_text("")
    gh = tmp_path / "gh"
    gh.write_text(GH_STUB)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)

    proc = subprocess.run(
        ["bash", "-c", _retry_step()],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "GH_REPO": "ClickHouse/ClickHouse",
            "GH_TOKEN": "stub",
            "FIXTURE": str(fixture),
            # Lets the stub answer the first createdAt partition only; see GH_STUB.
            "LISTED_MARKER": str(tmp_path / "listed.marker"),
            "ATTEMPT1_JSON": json.dumps(
                {
                    "status": "completed",
                    "conclusion": attempt1_conclusion,
                    "run_attempt": 1,
                }
            ),
            "ATTEMPT1_FAIL": "1" if attempt1_fail else "",
            "JOBS_JSON": jobs_json,
            "RERUN_LOG": str(rerun_log),
        },
    )
    assert proc.returncode == 0, proc.stderr
    return proc, rerun_log.read_text()


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
@pytest.mark.parametrize(
    "attempt, attempt1_conclusion, age_hours, selected",
    [
        (1, "failure", 1, True),  # same-repo run, first real attempt
        (2, "action_required", 1, True),  # fork run, gate consumed attempt 1
        (2, "failure", 1, False),  # genuinely retried once already
        (3, "failure", 1, False),  # retried twice
        (3, "action_required", 1, False),  # gated, then retried: past the attempt bound
        (1, "failure", 9, False),  # first attempt but older than the cutoff
    ],
)
def test_eligibility(tmp_path, attempt, attempt1_conclusion, age_hours, selected):
    """Assert whether the step considers a run at all (selector coverage)."""
    proc, _ = _run_step(tmp_path, attempt, attempt1_conclusion, age_hours, NO_JOBS)
    considered = f"actions/runs/{RUN_ID} " in proc.stdout
    assert considered is selected, proc.stdout


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
@pytest.mark.parametrize(
    "age_hours, started_age_hours, selected",
    [
        # The gate held the run 9 hours, so createdAt is outside the 6 hour window while the
        # attempt that actually ran started inside it. Reading createdAt drops it.
        (9, 1, True),
        # Both stamps inside: admitted either way (this arm is the control that keeps the
        # case above from passing merely because the selector admits everything).
        (1, 1, True),
        # Both stamps outside: still rejected, so widening the field did not disable the
        # window. Without this arm "startedAt" and "always true" are indistinguishable.
        (9, 9, False),
        # startedAt outside but createdAt inside -- the inverse skew. A predicate reading
        # createdAt admits it; one reading startedAt does not. The attempt that ran is stale,
        # so rejecting is correct and this arm fails if the two fields are merely OR'd.
        (1, 9, False),
    ],
)
def test_window_keys_on_the_attempt_that_ran(
    tmp_path, age_hours, started_age_hours, selected
):
    """The 6 hour window must be measured on startedAt, not createdAt.

    GitHub pins ``createdAt`` to the approval-gated attempt 1 and advances ``startedAt`` on
    the attempt that actually executed jobs (observed 22.98h apart at the extreme, 22% of
    failed runs skewed by over a minute). Keying on ``createdAt`` makes a fork PR that sat in
    the gate longer than the window invisible to the retry -- the class this workflow exists
    to cover.
    """
    proc, _ = _run_step(
        tmp_path,
        2,
        "action_required",
        age_hours,
        NO_JOBS,
        started_age_hours=started_age_hours,
    )
    considered = f"actions/runs/{RUN_ID} " in proc.stdout
    assert considered is selected, proc.stdout


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
def test_candidates_are_processed_closest_to_leaving_the_window_first(tmp_path):
    """``MAX_RERUNS`` bites the candidate list, so its order decides who is reached.

    A candidate is admitted by a 6 hour ``startedAt`` window, so the one nearest that edge
    must come first: this tick is its last chance, while a fresher one is still admitted by
    later ticks. Ordering by the leading ``databaseId`` instead only approximates that (a
    0.90 pairwise agreement with ``startedAt`` order over 4711 runs) and reaches fewer
    last-tick candidates in either direction.

    Asserted on the OBSERVED processing order rather than the sort command's spelling --
    pinning a literal shape is what let the previous ordering defect through.
    """
    # startedAt deliberately DISAGREES with databaseId order, so an id-keyed sort in either
    # direction produces an order this assertion rejects: the oldest startedAt carries the
    # HIGHEST id and the newest the middle one.
    rows = [
        (5_000_000_003, 5.5),  # closest to the 6h edge -> must be processed FIRST
        (5_000_000_001, 3.0),
        (5_000_000_002, 1.0),  # freshest -> LAST
    ]
    fixture = tmp_path / "runs.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "databaseId": rid,
                    "attempt": 1,
                    "createdAt": _stamp(started),
                    "startedAt": _stamp(started),
                }
                for rid, started in rows
            ]
        )
    )
    gh = tmp_path / "gh"
    gh.write_text(GH_STUB)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
    rerun_log = tmp_path / "reruns.log"
    rerun_log.write_text("")
    proc = subprocess.run(
        ["bash", "-c", _retry_step()],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "GH_REPO": "ClickHouse/ClickHouse",
            "GH_TOKEN": "stub",
            "FIXTURE": str(fixture),
            "LISTED_MARKER": str(tmp_path / "listed.marker"),
            "ATTEMPT1_JSON": json.dumps(
                {"status": "completed", "conclusion": "failure", "run_attempt": 1}
            ),
            "ATTEMPT1_FAIL": "",
            "JOBS_JSON": NO_JOBS,
            "RERUN_LOG": str(rerun_log),
        },
    )
    assert proc.returncode == 0, proc.stderr
    seen = re.findall(r"actions/runs/(\d+) ", proc.stdout)
    # All three must be reached, or the order assertion below could hold on a subset.
    assert sorted(seen) == sorted(str(r) for r, _ in rows), (seen, proc.stdout)
    want = [str(rid) for rid, _ in sorted(rows, key=lambda r: -r[1])]
    assert seen == want, ("candidates must be processed oldest-startedAt first", seen, want)


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
@pytest.mark.parametrize(
    "attempt1_conclusion, rerun_expected",
    [
        ("action_required", True),  # gated fork run: the case this gate exists for
        ("failure", False),  # already retried: rejected despite an infra-looking failure
    ],
)
def test_gated_fork_run_is_actually_rerun(tmp_path, attempt1_conclusion, rerun_expected):
    """An attempt-2 candidate with an infra failure must reach ``gh run rerun`` iff the
    attempt-1 probe says its first attempt was the approval gate."""
    proc, rerun_log = _run_step(tmp_path, 2, attempt1_conclusion, 1, INFRA_JOBS)
    assert (f"run rerun {RUN_ID}" in rerun_log) is rerun_expected, (
        proc.stdout,
        rerun_log,
    )
    if rerun_expected:
        # The fixture is attempt 2, so the rerun it triggers is attempt 3.
        assert "/attempts/3" in proc.stdout, proc.stdout


@pytest.mark.skipif(
    not shutil.which("jq"),
    reason="needs jq; absent from the CI Tests image, so CI relies on the static asserts",
)
def test_attempt1_probe_fails_closed_on_api_error(tmp_path):
    """A failing attempt-1 API call must skip the run, not abort the whole tick: the empty
    fallback value is not ``action_required``, so the candidate is rejected and the loop
    continues. Without the fallback, ``set -euo pipefail`` would kill the step instead."""
    proc, rerun_log = _run_step(
        tmp_path, 2, "action_required", 1, INFRA_JOBS, attempt1_fail=True
    )
    # _run_step already asserts returncode 0, i.e. the step survived the API error.
    # The empty conclusion in the skip message is the fallback's own value, so this also
    # proves the probe was reached rather than the run being dropped earlier.
    assert f"Skipping run {RUN_ID}: attempt 1 conclusion ''" in proc.stdout, proc.stdout
    # Annotated, so a token or endpoint fault that skips every fork candidate is visible on a
    # run that still reports success, and worded so an unreadable probe is not reported as an
    # observed retry.
    assert "::warning::" in proc.stdout, proc.stdout
    assert "already retried" not in proc.stdout, proc.stdout
    assert f"run rerun {RUN_ID}" not in rerun_log, rerun_log
