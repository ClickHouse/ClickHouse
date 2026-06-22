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
#   - `gh` is aliased to `history | fzf` in some interactive shells, so this
#     script always calls `command gh` to reach the real GitHub CLI.
#   - Read-only: it never closes PRs, dispatches workflows, or writes anything.
#   - ponytail: classification is a grep on the failed-step log + one job-info
#     lookup for cancelled runs — good enough; deepen only if a case is ambiguous.

set -uo pipefail

REPO="ClickHouse/ClickHouse"
DAYS="${1:-14}"
STALE_DAYS="${STALE_DAYS:-18}"   # a targeted version older than this with new commits == MISSING

# ponytail: hardcoded exclusion per request (e.g. an LTS handled out-of-band).
# Override with EXCLUDE_VERSIONS="" (analyze everything) or a different list.
# Revisit every release cycle — version numbers go stale, and a skip can silently
# hide a real gap once that line moves out of support.
EXCLUDE_VERSIONS="${EXCLUDE_VERSIONS:-25.8}"

GH() { command gh "$@"; }

# ---- preflight self-check (fail-close: no fabricated data) -------------------
if ! command -v gh >/dev/null 2>&1; then
    echo "ERROR: gh CLI not found on PATH." >&2; exit 2
fi
if ! GH auth status >/dev/null 2>&1; then
    echo "ERROR: gh is not authenticated (run: command gh auth login)." >&2; exit 2
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
RELEASES_JSON="$(GH api "repos/$REPO/releases?per_page=100" --jq '[.[] | {tag: .tag_name, published: .published_at}]' 2>/dev/null)"
printf "   %-7s %-26s %6s  %-10s %s\n" "branch" "latest patch" "age" "commits" "verdict"
printf "   %-7s %-26s %6s  %-10s %s\n" "------" "------------" "---" "-------" "-------"
MISSING_LIST=""
for v in $VERSIONS; do
    # newest tag for this major (sorted by published_at), via python over the releases list
    read -r TAG PUBLISHED AGE < <(printf '%s' "$RELEASES_JSON" | NOW="$NOW_EPOCH" python3 -c '
import sys, os, json, datetime
v = sys.argv[1]
rel = json.load(sys.stdin)
cands = [r for r in rel if r["tag"].startswith("v" + v + ".")]
if not cands:
    print("none - 0"); sys.exit()
cands.sort(key=lambda r: r["published"] or "", reverse=True)
top = cands[0]
pub = datetime.datetime.strptime(top["published"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
age = (int(os.environ["NOW"]) - int(pub.timestamp())) // 86400
print(top["tag"], top["published"][:10], age)
' "$v")

    if [[ "$TAG" == "none" ]]; then
        printf "   %-7s %-26s %6s  %-10s %s\n" "$v" "(no release found)" "-" "-" "⚠️  no tag"
        continue
    fi
    AHEAD="$(GH api "repos/$REPO/compare/${TAG}...${v}" --jq '.ahead_by' 2>/dev/null)"
    [[ -z "$AHEAD" ]] && AHEAD="?"

    verdict="ok"
    if [[ "$AGE" -gt "$STALE_DAYS" && "$AHEAD" != "0" && "$AHEAD" != "?" ]]; then
        verdict="⚠️  MISSING (stale + unreleased commits)"
        MISSING_LIST="$MISSING_LIST $v"
    fi
    printf "   %-7s %-26s %5sd  %-10s %s\n" "$v" "$TAG" "$AGE" "$AHEAD" "$verdict"
done
echo
if [[ -n "${MISSING_LIST// /}" ]]; then
    echo "   VERDICT: missing/overdue ->$MISSING_LIST"
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
    --json databaseId,conclusion,status,createdAt 2>/dev/null)"
n_guard=0; n_runner=0; n_other=0; n_ok=0
printf "   %-12s %-12s %-12s %s\n" "date" "conclusion" "class" "run-id"
while IFS=$'\t' read -r id concl stat created; do
    [[ -z "$id" ]] && continue
    day="${created:0:10}"
    cls=""
    case "$concl" in
        success)
            cls="ok"; n_ok=$((n_ok+1)) ;;
        failure)
            # GUARD == AutoReleaseInfo died at auto_release.py:107 / raise RuntimeError
            if GH run view "$id" --repo "$REPO" --log-failed 2>/dev/null \
                 | grep -qE 'auto_release\.py", line 1[0-9][0-9]|raise RuntimeError'; then
                cls="GUARD"; n_guard=$((n_guard+1))
            else
                cls="OTHER"; n_other=$((n_other+1))
            fi ;;
        cancelled|"")
            # RUNNER == first job never got a runner (empty steps / no runner_name)
            jid="$(GH run view "$id" --repo "$REPO" --json jobs --jq '.jobs[0].databaseId' 2>/dev/null)"
            info="$(GH api "repos/$REPO/actions/jobs/$jid" --jq '"\(.runner_name // "")|\(.steps|length)"' 2>/dev/null)"
            runner="${info%%|*}"; nsteps="${info##*|}"
            if [[ -z "$runner" && "${nsteps:-0}" == "0" ]]; then
                cls="RUNNER"; n_runner=$((n_runner+1))
            else
                cls="cancelled"
            fi ;;
        *) cls="$concl" ;;
    esac
    printf "   %-12s %-12s %-12s %s\n" "$day" "${concl:-$stat}" "$cls" "$id"
done < <(printf '%s' "$AR_RUNS" | python3 -c '
import sys, json
for r in json.load(sys.stdin):
    print("\t".join([str(r["databaseId"]), r.get("conclusion") or "", r.get("status") or "", r.get("createdAt") or ""]))
')
echo "   tally: GUARD=$n_guard  RUNNER=$n_runner  OTHER=$n_other  ok=$n_ok"
echo

echo "== 3b. CreateRelease runs since $SINCE (manual dispatches / matrix calls) =="
GH run list --repo "$REPO" --workflow create_release.yml --created ">=$SINCE" -L 40 \
    --json databaseId,conclusion,status,createdAt,event \
    --jq '.[] | "   \(.createdAt[0:10])  \(.conclusion // .status)  \(.event)  \(.databaseId)"' 2>/dev/null \
    || echo "   (none)"
echo

# ============================================================================
# 4. Live guard re-check — exactly the query auto_release.py runs
# ============================================================================
echo "== 4. Guard query NOW (open PRs matching \"Update version_date.tsv\") =="
GUARD="$(GH pr list --repo "$REPO" --state open --search "Update version_date.tsv" \
    --json number,title,author,headRefName \
    --jq '.[] | "   #\(.number)  [\(.author.login)]  \(.headRefName)  \(.title)"' 2>/dev/null)"
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
