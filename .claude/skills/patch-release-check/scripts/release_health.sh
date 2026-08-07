#!/usr/bin/env bash
# release_health.sh — read-only health check for ClickHouse stable patch releases.
#
# Reproduces the deterministic data-gathering of the patch-release-check skill:
#   1. Supported (targeted) versions, read live from SECURITY.md (never hardcoded)
#   2. Per-version staleness: latest patch tag, age, unreleased commits, MISSING flag
#   3. Failure scan of the AutoReleases / CreateRelease workflows over a window,
#      with each failure classified GUARD (version-bump-PR guard) / RUNNER
#      (no release-maker runner) / OTHER
#   4. Live re-check of the guard query (the set of PRs currently wedging releases)
#
# Usage:  bash release_health.sh [lookback_days]      # default 14
#
# Notes:
#   - All GitHub calls go through the GH() wrapper, which execs the gh binary with
#     GH_CONFIG_DIR dropped (see its definition for why).
#   - Read-only: it never closes PRs, dispatches workflows, or writes anything.
#   - ponytail: classification is a grep on the failed-step log + one job-info
#     lookup for cancelled runs — good enough; deepen only if a case is ambiguous.

set -uo pipefail

REPO="ClickHouse/ClickHouse"
DAYS="${1:-14}"
STALE_DAYS="${STALE_DAYS:-18}"   # a targeted version older than this with new commits == MISSING

# ponytail: hardcoded exclusion per request (e.g. an LTS handled out-of-band).
# Override with EXCLUDE_VERSIONS="" (analyze everything) or a different list.
# Uses ${VAR-default} (not :-) so an explicit empty value IS respected.
# Revisit every release cycle — version numbers go stale, and a skip can silently
# hide a real gap once that line moves out of support; the verdict names what was
# excluded so an overdue version is never hidden behind a green "all healthy".
EXCLUDE_VERSIONS="${EXCLUDE_VERSIONS-25.8}"

# Call the gh binary via `env`, dropping GH_CONFIG_DIR: some agent/runner checkouts
# point it at a config whose token passes `gh auth status` but 403s on real API calls
# — unsetting it falls back to the default config / GH_TOKEN that actually works (no-op
# when already unset). Use `env -u … gh`, NOT `env -u … command gh`: `command` is a
# shell builtin, so env would try to exec a binary named `command` and fail on systems
# that lack /usr/bin/command (e.g. Linux runners). A non-interactive script does not
# expand the interactive `gh` alias, so calling `gh` directly already hits the binary.
GH() { env -u GH_CONFIG_DIR gh "$@"; }

# Fail-close validator for a required read: the payload must parse as JSON, else
# abort. An empty JSON array ([]) is a legitimate result (no runs / no PRs) and
# passes; an empty string or garbled output (API/auth outage) does not. Call in
# the main shell (`need_json "$X" "desc" || exit 3`) — exiting inside a command
# substitution would only kill the subshell.
need_json() {  # need_json <payload> <description>
    if ! printf '%s' "$1" | python3 -c 'import sys, json; json.load(sys.stdin)' 2>/dev/null; then
        echo "ERROR: could not fetch $2 (API/auth failure or invalid JSON) — aborting (fail-close)." >&2
        return 1
    fi
}

# Fetch a run's failed-step log, accepting it only when COMPLETE. `--log-failed`
# is multi-MB and under bulk/rate-limited use it can return a non-empty but
# truncated stream missing the trailing traceback — which would misclassify a
# GUARD failure as OTHER. A failed step always ends with the GitHub Actions
# marker `Process completed with exit code`, so we retry until we see it and
# return non-zero if we never get a complete log (caller marks UNKNOWN, not OTHER).
fetch_failed_log() {  # fetch_failed_log <run-id>  -> prints complete log, rc 0; else rc 1
    local id="$1" log=""
    for _attempt in 1 2 3; do
        log="$(GH run view "$id" --repo "$REPO" --log-failed 2>/dev/null)"
        if printf '%s' "$log" | grep -q 'Process completed with exit code'; then
            printf '%s' "$log"
            return 0
        fi
    done
    return 1
}

# ---- preflight self-check (fail-close: no fabricated data) -------------------
if ! command -v gh >/dev/null 2>&1; then
    echo "ERROR: gh CLI not found on PATH." >&2; exit 2
fi
# gh writes a cache (default ~/.cache); some sandboxes mount a read-only HOME, where
# `gh run view --log-failed` fails at `mkdir ~/.cache` BEFORE reaching GitHub — which
# would silently turn every failed run into UNKNOWN. Point gh at a writable cache:
# respect an existing writable XDG_CACHE_HOME, else fall back to a repo-local dir.
_cache="${XDG_CACHE_HOME:-$HOME/.cache}"
if ! ( mkdir -p "$_cache" && [ -w "$_cache" ] ) 2>/dev/null; then
    _cache="$PWD/tmp/gh-cache"
    if ! mkdir -p "$_cache" 2>/dev/null; then
        echo "ERROR: no writable gh cache dir (tried \$XDG_CACHE_HOME/~/.cache and $_cache)." >&2
        exit 2
    fi
fi
export XDG_CACHE_HOME="$_cache"
if ! GH auth status >/dev/null 2>&1; then
    echo "ERROR: gh is not authenticated (run: gh auth login)." >&2; exit 2
fi
# Prove the repo is readable up front. `gh api repos/<repo>` exits non-zero on a
# not-found / no-access / API outage (unlike `gh pr list`, which returns [] exit 0),
# so this single check stops an auth/connectivity failure from later surfacing as a
# valid-but-empty result that reads as "no failures / guard clear".
if ! GH api "repos/$REPO" --jq '.full_name' >/dev/null 2>&1; then
    echo "ERROR: cannot read repo $REPO (not found / no access / API outage) — aborting (fail-close)." >&2; exit 2
fi

NOW_EPOCH="$(date -u +%s)"
echo "ClickHouse patch-release health  —  repo $REPO  —  window ${DAYS}d  —  $(date -u +%Y-%m-%dT%H:%MZ)"
echo

# ============================================================================
# 1. Targeted versions = the ✔️ rows of SECURITY.md
# ============================================================================
echo "== 1. Targeted versions (SECURITY.md ✔️) =="
SECMD="$(GH api "repos/$REPO/contents/SECURITY.md" -H "Accept: application/vnd.github.raw" 2>/dev/null)"
VERSIONS="$(printf '%s' "$SECMD" | python3 -c '
import sys, re
out = []
for line in sys.stdin:
    m = re.match(r"\s*\|\s*([0-9]+\.[0-9]+)\s*\|\s*(.+?)\s*\|", line)
    if m and "✔" in m.group(2):   # ✔️
        out.append(m.group(1))
print(" ".join(out))
')"
if [[ -z "${VERSIONS// /}" ]]; then
    echo "ERROR: could not parse any supported versions from SECURITY.md — aborting (fail-close)." >&2
    exit 3
fi
echo "   supported: $VERSIONS"
# Apply hardcoded exclusion(s).
if [[ -n "${EXCLUDE_VERSIONS// /}" ]]; then
    KEPT=""
    for v in $VERSIONS; do
        skip=0
        for e in $EXCLUDE_VERSIONS; do [[ "$v" == "$e" ]] && skip=1; done
        [[ "$skip" == "0" ]] && KEPT="$KEPT $v"
    done
    VERSIONS="${KEPT# }"
    echo "   excluded:  $EXCLUDE_VERSIONS  (EXCLUDE_VERSIONS)"
fi
echo "   analyzed:  $VERSIONS"
echo

# ============================================================================
# 2. Per-version staleness
# ============================================================================
echo "== 2. Per-version staleness (latest patch, age, unreleased commits) =="
printf "   %-7s %-26s %6s  %-10s %s\n" "branch" "latest patch" "age" "rel/tot" "verdict"
printf "   %-7s %-26s %6s  %-10s %s\n" "------" "------------" "---" "-------" "-------"
MISSING_LIST=""; NOTAG_LIST=""
for v in $VERSIONS; do
    # Resolve the newest tag for this major from git/matching-refs — this is the
    # COMPLETE tag list for the branch, not a single recent-releases page, so a
    # quiet/older supported LTS is never falsely seen as having no release. Sort by
    # numeric version tuple (lexical sort would rank v25.8.9 above v25.8.24).
    REFS="$(GH api "repos/$REPO/git/matching-refs/tags/v${v}." 2>/dev/null)" \
        || { echo "ERROR: failed to list tags for v$v (gh exited non-zero) — aborting (fail-close)." >&2; exit 3; }
    need_json "$REFS" "tags for v$v" || exit 3
    TAGINFO="$(printf '%s' "$REFS" | python3 -c '
import sys, json, re
data = json.load(sys.stdin)
m = {r["ref"].split("refs/tags/")[-1]: r["object"]["sha"] for r in data}
def key(t):
    mm = re.match(r"v(\d+)\.(\d+)\.(\d+)\.(\d+)", t)
    return tuple(int(x) for x in mm.groups()) if mm else (-1,)
refs = [t for t in m if re.match(r"v\d+\.\d+\.\d+\.\d+", t)]
t = sorted(refs, key=key)[-1] if refs else ""
print(t, (m.get(t, "") if t else ""))
')"
    TAG="${TAGINFO%% *}"; TAGSHA="${TAGINFO##* }"
    # Fail-closed: a supported version with NO release tag must make the final
    # verdict non-green — never silently pass as healthy.
    if [[ -z "$TAG" ]]; then
        NOTAG_LIST="$NOTAG_LIST $v"
        printf "   %-7s %-26s %6s  %-10s %s\n" "$v" "(no release tag)" "-" "-" "⚠️  NO TAG — investigate"
        continue
    fi
    # Age from the annotated tag object's tagger date — exactly what auto_release.py
    # uses (repo.get_git_tag(ref.object.sha).tagger.date). Fall back to the GitHub
    # Release publish date only if the tag object carries no date (e.g. a lightweight
    # tag): a valid recent git tag alone is healthy, even with no published Release.
    TAGDATE="$(GH api "repos/$REPO/git/tags/$TAGSHA" --jq '.tagger.date' 2>/dev/null)"
    if ! [[ "$TAGDATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]]; then
        TAGDATE="$(GH api "repos/$REPO/releases/tags/$TAG" --jq '.published_at' 2>/dev/null)"
    fi
    AGE="$(NOW="$NOW_EPOCH" PUB="$TAGDATE" python3 -c '
import os, datetime
s = os.environ["PUB"].strip().replace("Z", "+00:00")
try:
    pub = datetime.datetime.fromisoformat(s)
except ValueError:
    raise SystemExit(1)
if pub.tzinfo is None:
    pub = pub.replace(tzinfo=datetime.timezone.utc)
print((int(os.environ["NOW"]) - int(pub.timestamp())) // 86400)
' 2>/dev/null)"
    # Fail-closed: if neither the tag object nor a Release yields a parseable date we
    # cannot assess staleness — treat as no-tag, never default to a green "ok".
    if ! [[ "$AGE" =~ ^-?[0-9]+$ ]]; then
        NOTAG_LIST="$NOTAG_LIST $v"
        printf "   %-7s %-26s %6s  %-10s %s\n" "$v" "$TAG" "-" "-" "⚠️  could not read tag date"
        continue
    fi
    # Release-worthy commits, reproducing AutoReleaseInfo: it takes the FIRST-PARENT
    # commits of <tag>..<branch>, drops the oldest (post-release version-bump) commit,
    # and a branch with no non-bump commit has nothing to release. The raw compare set
    # / ahead_by counts side commits off the first-parent chain and over-reports, so we
    # reconstruct the first-parent chain from the compare payload's parents (no extra
    # API calls). rel/tot = release-worthy / first-parent commits since the tag.
    CMP="$(GH api "repos/$REPO/compare/${TAG}...${v}" 2>/dev/null)"
    read -r WORTHY FPTOTAL < <(printf '%s' "$CMP" | python3 -c '
import sys, json, re
d = json.load(sys.stdin)
ahead = int(d["ahead_by"])
commits = d.get("commits", [])
bump = re.compile(r"^(Update autogenerated version|Update version_date\.tsv)")
by = {c["sha"]: c for c in commits}
chain = []
if commits:
    cur = commits[-1]["sha"]; seen = set()
    while cur in by and cur not in seen:
        seen.add(cur); c = by[cur]
        chain.append(c["commit"]["message"].splitlines()[0])
        p = c.get("parents", []); cur = p[0]["sha"] if p else None
# Fail-closed: a non-empty compare that yields no first-parent chain means the
# reconstruction is wrong — abort rather than risk under-counting (hiding a gap).
if commits and not chain:
    raise SystemExit(1)
cand = chain[:-1] if chain else []   # drop oldest = the version-bump commit
worthy = sum(1 for m in cand if not bump.match(m))
# If the compare payload was truncated (>~250 reachable commits) there are certainly
# real backports beyond what we walked, so never report 0.
if ahead > len(commits):
    worthy = max(worthy, 1)
print(worthy, len(chain))
' 2>/dev/null)
    # Fail-close: a failed/garbled compare must NOT be silently treated as "ok".
    if ! [[ "$WORTHY" =~ ^[0-9]+$ && "$FPTOTAL" =~ ^[0-9]+$ ]]; then
        echo "ERROR: could not compute first-parent commits for ${TAG}...${v} (API failure / unexpected payload) — aborting (fail-close)." >&2
        exit 3
    fi

    verdict="ok"
    if [[ "$AGE" -gt "$STALE_DAYS" && "$WORTHY" != "0" ]]; then
        verdict="⚠️  MISSING (stale + $WORTHY release-worthy commit(s))"
        MISSING_LIST="$MISSING_LIST $v"
    elif [[ "$AGE" -gt "$STALE_DAYS" && "$WORTHY" == "0" ]]; then
        verdict="ok (stale but only a version-bump commit — nothing to release)"
    fi
    printf "   %-7s %-26s %5sd  %-10s %s\n" "$v" "$TAG" "$AGE" "$WORTHY/$FPTOTAL" "$verdict"
done
echo
# A supported version with no resolvable release tag is a hard failure, not a
# warning row: never let the run end green after printing "NO TAG".
if [[ -n "${NOTAG_LIST// /}" ]]; then
    echo "   VERDICT: FAILED — no release tag for:$NOTAG_LIST (cannot confirm release health)"
    [[ -n "${MISSING_LIST// /}" ]] && echo "   also missing/overdue ->$MISSING_LIST"
    exit 3
elif [[ -n "${MISSING_LIST// /}" ]]; then
    echo "   VERDICT: missing/overdue ->$MISSING_LIST"
elif [[ -n "${EXCLUDE_VERSIONS// /}" ]]; then
    echo "   VERDICT: all ANALYZED versions have a recent patch — NOT checked (excluded):$EXCLUDE_VERSIONS"
else
    echo "   VERDICT: all targeted versions have a recent patch."
fi
echo

# ============================================================================
# 3. Workflow failure scan + classification
# ============================================================================
SINCE="$(python3 -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=int(sys.argv[1]))).strftime("%Y-%m-%d"))' "$DAYS")"

echo "== 3a. AutoReleases runs since $SINCE (the scheduler that should release) =="
AR_RUNS="$(GH run list --repo "$REPO" --workflow auto_releases.yml --created ">=$SINCE" -L 60 \
    --json databaseId,conclusion,status,createdAt 2>/dev/null)" \
    || { echo "ERROR: failed to list AutoReleases runs (gh exited non-zero) — aborting (fail-close)." >&2; exit 3; }
need_json "$AR_RUNS" "AutoReleases run list" || exit 3
n_guard=0; n_runner=0; n_other=0; n_ok=0; n_unknown=0
printf "   %-12s %-12s %-12s %s\n" "date" "conclusion" "class" "run-id"
while IFS='|' read -r id concl stat created; do
    [[ -z "$id" ]] && continue
    day="${created:0:10}"
    cls=""
    case "$concl" in
        success)
            cls="ok"; n_ok=$((n_ok+1)) ;;
        failure)
            # GUARD == the version-bump-PR guard specifically: the traceback must
            # show both the `_prepare` frame AND a `raise RuntimeError` source line.
            # Match the combination, NOT a line number (which drifts) and NOT bare
            # `raise RuntimeError` — other `_prepare` failures (e.g. the `assert refs`
            # candidate check) raise AssertionError and must classify as OTHER.
            if flog="$(fetch_failed_log "$id")"; then
                # Use here-strings, NOT `printf "$flog" | grep -q`: grep -q exits on
                # first match and SIGPIPEs the producer (rc 141), which `set -o pipefail`
                # propagates, short-circuiting the && to the OTHER branch — silently
                # misclassifying a real GUARD failure on a multi-MB log.
                if grep -q 'in _prepare' <<<"$flog" \
                   && grep -q 'raise RuntimeError' <<<"$flog"; then
                    cls="GUARD"; n_guard=$((n_guard+1))
                else
                    cls="OTHER"; n_other=$((n_other+1))
                fi
            else
                # fail-closed: couldn't obtain a complete log, so we cannot rule out
                # (or confirm) the guard — never silently downgrade to OTHER.
                cls="UNKNOWN"; n_unknown=$((n_unknown+1))
            fi ;;
        cancelled)
            # RUNNER == a COMPLETED cancelled run whose first job never got a runner
            # (no runner_name, zero steps) — the ~24h-queued-then-cron-cancelled case.
            # Only a genuinely cancelled run qualifies; queued/in_progress runs have an
            # empty conclusion and are handled in the "" arm below, not here.
            if [[ "$stat" != "completed" ]]; then
                cls="cancelled"   # cancelled but not yet finalized; don't diagnose
            else
                jid="$(GH run view "$id" --repo "$REPO" --json jobs --jq '.jobs[0].databaseId' 2>/dev/null)"
                info="$(GH api "repos/$REPO/actions/jobs/$jid" --jq '"\(.runner_name // "")|\(.steps|length)"' 2>/dev/null)"
                # A successful job-metadata read always looks like "<runner>|<N>"
                # (N numeric). Anything else means the read failed — fail-closed to
                # UNKNOWN rather than calling a fetch failure a runner outage.
                if [[ -z "$jid" || ! "$info" =~ \|[0-9]+$ ]]; then
                    cls="UNKNOWN"; n_unknown=$((n_unknown+1))
                else
                    runner="${info%%|*}"; nsteps="${info##*|}"
                    if [[ -z "$runner" && "$nsteps" == "0" ]]; then
                        cls="RUNNER"; n_runner=$((n_runner+1))
                    else
                        cls="cancelled"
                    fi
                fi
            fi ;;
        "")
            # No conclusion yet => the run is still queued / in_progress. Report its
            # live status; do NOT classify it as a runner outage (a queued daily run
            # legitimately has no runner/steps yet).
            cls="${stat:-pending}" ;;
        *)  # Any other non-success conclusion (startup_failure, timed_out,
            # action_required, neutral, ...) is a real anomaly — count it as OTHER so
            # the tally can never read all-zeros while a scheduled run actually failed.
            cls="$concl"; n_other=$((n_other+1)) ;;
    esac
    printf "   %-12s %-12s %-12s %s\n" "$day" "${concl:-$stat}" "$cls" "$id"
done < <(printf '%s' "$AR_RUNS" | python3 -c '
import sys, json
for r in json.load(sys.stdin):
    print("|".join([str(r["databaseId"]), r.get("conclusion") or "", r.get("status") or "", r.get("createdAt") or ""]))
')
echo "   tally: GUARD=$n_guard  RUNNER=$n_runner  OTHER=$n_other  UNKNOWN=$n_unknown  ok=$n_ok"
[[ "$n_other" -gt 0 ]] && echo "   note: OTHER = a non-guard failure or an unexpected conclusion (startup_failure/timed_out/action_required/...) — read its log; startup_failure/timed_out usually mean the release-maker runner never came up or the job timed out."
[[ "$n_unknown" -gt 0 ]] && echo "   note: UNKNOWN = required GitHub data (failed-step log or job metadata) could not be fetched — investigate manually; do not assume it is healthy / not a guard failure."
echo

echo "== 3b. CreateRelease runs since $SINCE (manual dispatches / matrix calls) =="
CR_RUNS="$(GH run list --repo "$REPO" --workflow create_release.yml --created ">=$SINCE" -L 40 \
    --json databaseId,conclusion,status,createdAt,event 2>/dev/null)" \
    || { echo "ERROR: failed to list CreateRelease runs (gh exited non-zero) — aborting (fail-close)." >&2; exit 3; }
need_json "$CR_RUNS" "CreateRelease run list" || exit 3
printf '%s' "$CR_RUNS" | python3 -c '
import sys, json
runs = json.load(sys.stdin)
if not runs:
    print("   (none in window)")
for r in runs:
    print("   {}  {}  {}  {}".format(
        (r.get("createdAt") or "")[:10], r.get("conclusion") or r.get("status"),
        r.get("event"), r.get("databaseId")))
'
echo

# ============================================================================
# 4. Live guard re-check — exactly the query auto_release.py runs
# ============================================================================
echo "== 4. Guard query NOW (open PRs matching \"Update version_date.tsv\") =="
GUARD_JSON="$(GH pr list --repo "$REPO" --state open --search "Update version_date.tsv" \
    --json number,title,author,headRefName 2>/dev/null)" \
    || { echo "ERROR: failed to run guard query (gh exited non-zero) — aborting (fail-close)." >&2; exit 3; }
need_json "$GUARD_JSON" "guard PR query" || exit 3
GUARD="$(printf '%s' "$GUARD_JSON" | python3 -c '
import sys, json
for p in json.load(sys.stdin):
    print("   #{}  [{}]  {}  {}".format(p["number"], p["author"]["login"], p["headRefName"], p["title"]))
')"
if [[ -z "$GUARD" ]]; then
    echo "   [] — guard is CLEAR; AutoReleaseInfo will not RuntimeError on this check."
else
    echo "$GUARD"
    echo
    echo "   ⚠️  Non-empty => the next AutoReleaseInfo run will raise RuntimeError at this guard."
    echo "       Persistent blocker = a robot-clickhouse 'auto/v*' bump PR whose tag is superseded."
    echo "       Anything else (human PR matching the loose text search) is a false positive."
fi
echo
echo "Done. See SKILL.md for how to diagnose each class and the gated remediation steps."
