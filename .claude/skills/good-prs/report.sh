#!/usr/bin/env bash
#
# good-prs/report.sh — list open PRs whose ONLY non-green check is "CH Inc sync"
# (or that are fully green), so they are effectively ready to merge.
#
# Sections:
#   1. PRs you authored
#   2. PRs assigned to you, authored by others (excluding tracked authors)
#   3. one section per "tracked author" (default: groeneai), regardless of assignee
#
# For every PR we report:
#   - the "CH Inc sync" state: GREEN (also passed), FAILED, INPROG (still running),
#     or NOSYNC (no such check, e.g. release-branch backports)
#   - whether it conflicts with its base branch in the public repository: the sync
#     label is then suffixed with "+CONFLICT" (or "+UNKNOWN" when GitHub has not
#     computed mergeability yet), because such a PR cannot be merged as it is
#   - whether it was APPROVED by anyone at least once
# Already-merged / closed PRs are excluded.
#
# Usage:
#   report.sh [tracked-author ...]      # default tracked author: groeneai
#
set -euo pipefail

# Pin the repository explicitly so the report always describes ClickHouse/ClickHouse
# regardless of the directory the skill is run from. The output links below are also
# hardcoded to ClickHouse/ClickHouse, so the data source must match them.
REPO="ClickHouse/ClickHouse"
export REPO

TRACK_AUTHORS=("groeneai")
if [ "$#" -gt 0 ]; then TRACK_AUTHORS=("$@"); fi

ME=$(gh api user --jq .login)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
# The per-PR helpers below are separate scripts run by xargs, so they locate the
# shared retry wrapper through the environment rather than a relative path.
export GOOD_PRS_WORK="$WORK"

# ---- retry wrapper shared by both per-PR helpers ----------------------------
# A full report makes roughly two GraphQL calls per PR - several hundred per run -
# so hitting at least one transient GitHub failure is close to certain: the API
# regularly answers 502/503/504, and a wide fan-out can trip the secondary rate
# limit. Aborting on the first one throws away all the quota already spent, so
# those are retried with exponential backoff, and a primary rate-limit rejection
# waits for the hourly window to roll over instead.
#
# This does not weaken the fail-loud design: errors that will never fix
# themselves (auth, unknown PR, bad flag) still fail on the first attempt, and a
# transient error that outlives every attempt is still fatal. A row is never
# silently dropped either way.
cat > "$WORK/ghretry.sh" <<'SH'
GH_RETRIES=${GH_RETRIES:-5}
GH_RETRY_DELAY=${GH_RETRY_DELAY:-2}

# gh_retry <errfile> <cmd...>
# On success sets GH_OUT to the command's stdout and returns 0. On failure returns
# non-zero, leaving the last attempt's stderr in <errfile> for the caller to report.
gh_retry() {
  local errfile="$1"; shift
  local attempt=1 delay="$GH_RETRY_DELAY" reset now wait_s
  while :; do
    if GH_OUT=$("$@" 2>"$errfile"); then return 0; fi
    if [ "$attempt" -ge "$GH_RETRIES" ]; then return 1; fi
    if grep -qiE 'secondary rate limit|submitted too quickly' "$errfile"; then
      sleep "$delay"; delay=$(( delay * 2 + 1 ))
    elif grep -qiE 'rate limit' "$errfile"; then
      # Primary hourly limit: retrying before the window resets can only fail
      # again, so wait it out. The rate_limit endpoint is itself unmetered, and
      # the wait is capped so a run always terminates.
      reset=$(gh api rate_limit --jq .resources.graphql.reset 2>/dev/null) || reset=""
      case "$reset" in (*[!0-9]*|"") reset=0 ;; esac
      now=$(date +%s)
      wait_s=$(( reset - now + 5 ))
      if [ "$wait_s" -lt "$GH_RETRY_DELAY" ]; then wait_s="$GH_RETRY_DELAY"; fi
      if [ "$wait_s" -gt 900 ]; then wait_s=900; fi
      echo "good-prs: GraphQL rate limit reached, waiting ${wait_s}s for the window to reset (attempt $attempt/$GH_RETRIES)" >&2
      sleep "$wait_s"
    elif grep -qiE 'HTTP (408|429|5[0-9][0-9])|Service Unavailable|Bad Gateway|Gateway Time-?out|couldn.t respond|timed out|connection reset|unexpected EOF|TLS handshake' "$errfile"; then
      sleep "$delay"; delay=$(( delay * 2 ))
    else
      return 1   # permanent error: no point burning attempts on it
    fi
    attempt=$(( attempt + 1 ))
  done
}
SH

# ---- helper invoked per-PR by xargs: classify the checks --------------------
# Output: "<num>\t<sync-label>\t<#other-non-green>" where <sync-label> is one of
# GREEN / FAILED / INPROG / NOSYNC, or NO_CHECKS (with count 99, so it is excluded).
#
# Classification uses `gh pr checks`' own `bucket` field, which groups the many raw
# states (SUCCESS, FAILURE, TIMED_OUT, STARTUP_FAILURE, QUEUED, CANCELLED, SKIPPED,
# NEUTRAL, ...) into pass / fail / pending / skipping / cancel. Using the bucket
# avoids silently dropping a PR whose sync (or another check) is in a raw state we
# did not enumerate.
#
# Failure handling: with `--json`, `gh pr checks` exits 0 and prints a JSON array
# whenever it could read the checks (even if some are failing or pending). A
# non-zero exit therefore means either "no checks reported" (a legitimate empty
# result) or a real API / auth / rate-limit error. We treat the former as NO_CHECKS
# and abort the whole run on the latter (once gh_retry has exhausted its attempts),
# rather than silently dropping the row.
cat > "$WORK/classify.sh" <<'SH'
#!/usr/bin/env bash
set -uo pipefail
. "$GOOD_PRS_WORK/ghretry.sh"
n="$1"
err=$(mktemp)
trap 'rm -f "$err"' EXIT
if gh_retry "$err" gh pr checks "$n" --repo "$REPO" --json name,state,bucket; then
  json="$GH_OUT"
  if [ -z "$json" ] || [ "$json" = "[]" ]; then
    printf '%s\tNO_CHECKS\t99\n' "$n"; exit 0
  fi
elif grep -qi 'no checks reported' "$err"; then
  printf '%s\tNO_CHECKS\t99\n' "$n"; exit 0
else
  echo "good-prs: 'gh pr checks $n' failed: $(tr '\n' ' ' <"$err")" >&2
  exit 255   # 255 makes xargs stop immediately; set -e then aborts report.sh
fi
echo "$json" | jq -r --arg n "$n" '
  (map(select(.name=="CH Inc sync")) | .[0].bucket // "absent") as $sync
  | (map(select(.name!="CH Inc sync" and .name!="PR" and .name!="Mergeable Check"
               and .name!="A Sync (only for tests)"))) as $others
  # a non-sync check counts as "not green" unless its bucket is pass or skipping
  | ($others | map(select(.bucket!="pass" and .bucket!="skipping")) | length) as $nbad
  | (if   $sync=="pass"     then "GREEN"
     elif $sync=="fail"     then "FAILED"
     elif $sync=="cancel"   then "FAILED"
     elif $sync=="pending"  then "INPROG"
     else                        "NOSYNC"   # skipping / absent
     end) as $synclabel
  | "\($n)\t\($synclabel)\t\($nbad)"'
SH
chmod +x "$WORK/classify.sh"

# ---- helper invoked per-PR by xargs: state + ever-approved + mergeable ------
# Output: "<num>\t<STATE>\t<true|false>\t<MERGEABLE|CONFLICTING|UNKNOWN>"
# A valid PR always yields data here; an empty result means a real gh failure, so
# we surface the diagnostic and abort rather than silently dropping the row.
#
# `mergeable` comes from the same call as the state and the reviews, so reporting
# conflicts costs no extra quota - except when GitHub answers UNKNOWN, see below.
GH_MERGEABLE_TRIES=${GH_MERGEABLE_TRIES:-3}
GH_MERGEABLE_DELAY=${GH_MERGEABLE_DELAY:-2}
export GH_MERGEABLE_TRIES GH_MERGEABLE_DELAY
cat > "$WORK/approval.sh" <<'SH'
#!/usr/bin/env bash
set -uo pipefail
. "$GOOD_PRS_WORK/ghretry.sh"
n="$1"
err=$(mktemp)
trap 'rm -f "$err"' EXIT
attempt=1
while :; do
  if gh_retry "$err" gh pr view "$n" --repo "$REPO" --json state,reviews,mergeable \
    --jq '[.state, ([.reviews[].state] | any(. == "APPROVED")), .mergeable] | @tsv'; then
    out="$GH_OUT"
  else
    echo "good-prs: 'gh pr view $n' failed: $(tr '\n' ' ' <"$err")" >&2
    exit 255
  fi
  # GitHub computes mergeability lazily: for a PR nobody looked at recently the
  # first query only schedules the test merge and answers UNKNOWN. Asking again
  # shortly after returns the real verdict, so a conflicting PR is not reported
  # as clean just because it was asked about first. Only an OPEN PR is worth
  # re-asking - a closed or merged one is dropped from the report anyway.
  case "$out" in
    OPEN*UNKNOWN) ;;
    *) break ;;
  esac
  [ "$attempt" -ge "$GH_MERGEABLE_TRIES" ] && break
  sleep "$GH_MERGEABLE_DELAY"
  attempt=$(( attempt + 1 ))
done
printf '%s\t%s\n' "$n" "$out"
SH
chmod +x "$WORK/approval.sh"

# ---- gather metadata (number, author, title) for each query group ----------
gh pr list --repo "$REPO" --author "$ME"   --state open --limit 1000 --json number,title,author \
  --jq '.[]|"\(.number)\t\(.author.login)\t\(.title)"' > "$WORK/authored.meta"
gh pr list --repo "$REPO" --assignee "$ME" --state open --limit 1000 --json number,title,author \
  --jq '.[]|"\(.number)\t\(.author.login)\t\(.title)"' > "$WORK/assigned.meta"
: > "$WORK/tracked.meta"
for a in "${TRACK_AUTHORS[@]}"; do
  gh pr list --repo "$REPO" --author "$a" --state open --limit 1000 --json number,title,author \
    --jq '.[]|"\(.number)\t\(.author.login)\t\(.title)"' >> "$WORK/tracked.meta"
done

# union of all PR numbers we care about
cat "$WORK"/*.meta | cut -f1 | sort -un > "$WORK/all_nums.txt"

# ---- classify checks + approvals for every PR (parallel) -------------------
# `-r` (--no-run-if-empty) is required: without it GNU xargs runs the helper
# once even on empty input, and the helper would read an unset "$1" and emit a
# bogus row. With `-r` the helper is skipped, and the redirect below still
# creates an empty output file, so the report renders clean empty sections when
# nobody has a qualifying PR (empty all_nums.txt) or none of them match the
# "only CH Inc sync is non-green" criterion (empty match_nums.txt).
#
# We deliberately do NOT discard the helpers' stderr: on a real gh failure the
# helper prints a diagnostic and exits 255, xargs propagates a non-zero status,
# and `set -e` aborts the run with that diagnostic visible.
xargs -r -P 12 -n 1 "$WORK/classify.sh" < "$WORK/all_nums.txt" > "$WORK/checks.tsv"
# Only PRs that match the criterion need an approval/state lookup
awk -F'\t' '$3==0 && ($2=="GREEN"||$2=="FAILED"||$2=="INPROG"||$2=="NOSYNC"){print $1}' \
  "$WORK/checks.tsv" | sort -un > "$WORK/match_nums.txt"
xargs -r -P 12 -n 1 "$WORK/approval.sh" < "$WORK/match_nums.txt" > "$WORK/appr.tsv"

# ---- render one markdown section -------------------------------------------
# args: <meta-file> <show-author-col:0|1> <exclude-authors-csv>
emit() {
  local meta="$1" showauth="$2" excl="$3"
  awk -F'\t' '$3==0 && ($2=="GREEN"||$2=="FAILED"||$2=="INPROG"||$2=="NOSYNC"){print $1"\t"$2}' \
    "$WORK/checks.tsv" \
  | while IFS=$'\t' read -r n st; do
      a=$(awk -F'\t' -v n="$n" '$1==n{print $2"\t"$3"\t"$4}' "$WORK/appr.tsv")
      state=$(echo "$a" | cut -f1); approved=$(echo "$a" | cut -f2)
      mergeable=$(echo "$a" | cut -f3)
      [ "$state" != "OPEN" ] && continue
      line=$(grep -m1 "^$n	" "$meta") || continue
      [ -z "$line" ] && continue
      auth=$(echo "$line" | cut -f2)
      title=$(echo "$line" | cut -f3 | sed 's/|/\\|/g')
      case ",$excl," in *",$auth,"*) continue;; esac
      case "$st" in
        FAILED) g=1; gl="FAILED";; INPROG) g=2; gl="INPROG";;
        GREEN)  g=3; gl="GREEN";;  NOSYNC) g=4; gl="NOSYNC";;
      esac
      # A PR that conflicts with its base branch cannot be merged whatever CI
      # says, so the conflict is shown in the same first column as the sync
      # state, and such rows are sorted after the clean ones within a bucket.
      case "$mergeable" in
        CONFLICTING) c=2; gl="$gl+CONFLICT";;
        MERGEABLE)   c=0;;
        *)           c=1; gl="$gl+UNKNOWN";;   # UNKNOWN, or absent for any reason
      esac
      [ "$approved" = "true" ] && av="yes" || av="-"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$g" "$c" "$gl" "$n" "$av" "$auth" "$title"
    done | sort -t$'\t' -k1,1n -k2,2n -k4,4n \
  | while IFS=$'\t' read -r g c gl n av auth title; do
      if [ "$showauth" = "1" ]; then
        printf '| %s | %s | [#%s](https://github.com/ClickHouse/ClickHouse/pull/%s) | %s | %s |\n' \
          "$gl" "$av" "$n" "$n" "$auth" "$title"
      else
        printf '| %s | %s | [#%s](https://github.com/ClickHouse/ClickHouse/pull/%s) | %s |\n' \
          "$gl" "$av" "$n" "$n" "$title"
      fi
    done
}

excl_csv=$(IFS=,; echo "${TRACK_AUTHORS[*]}")

echo "# Good PRs — only \`CH Inc sync\` blocking (or fully green)"
echo
echo "_Legend: GREEN = fully green / merge-ready · FAILED = only \`CH Inc sync\` failed · INPROG = only \`CH Inc sync\` still running · NOSYNC = no \`CH Inc sync\` check, everything else green (e.g. release-branch backports). A \`+CONFLICT\` suffix means the PR conflicts with its base branch in the public repository and needs a merge before it can go in; \`+UNKNOWN\` means GitHub had not finished computing mergeability. Approved = approved by someone at least once. Merged/closed PRs excluded._"
echo
echo "## 1. Your own PRs (authored by \`$ME\`)"
echo
echo "| sync / merge | approved | PR | title |"
echo "|--------------|:--:|----|-------|"
emit "$WORK/authored.meta" 0 ""
echo
echo "## 2. Assigned to you, authored by others (excluding: ${excl_csv})"
echo
echo "| sync / merge | approved | PR | author | title |"
echo "|--------------|:--:|----|--------|-------|"
emit "$WORK/assigned.meta" 1 "$ME,$excl_csv"
for a in "${TRACK_AUTHORS[@]}"; do
  echo
  echo "## 3. PRs by \`$a\` (regardless of assignee)"
  echo
  echo "| sync / merge | approved | PR | title |"
  echo "|--------------|:--:|----|-------|"
  grep -P "^\d+\t$a\t" "$WORK/tracked.meta" > "$WORK/one_author.meta" || true
  emit "$WORK/one_author.meta" 0 ""
done
