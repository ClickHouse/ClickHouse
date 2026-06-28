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
# and abort the whole run on the latter, rather than silently dropping the row.
cat > "$WORK/classify.sh" <<'SH'
#!/usr/bin/env bash
set -uo pipefail
n="$1"
err=$(mktemp)
trap 'rm -f "$err"' EXIT
if json=$(gh pr checks "$n" --repo "$REPO" --json name,state,bucket 2>"$err"); then
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

# ---- helper invoked per-PR by xargs: state + ever-approved -----------------
# Output: "<num>\t<STATE>\t<true|false>"
# A valid PR always yields data here; an empty result means a real gh failure, so
# we surface the diagnostic and abort rather than silently dropping the row.
cat > "$WORK/approval.sh" <<'SH'
#!/usr/bin/env bash
set -uo pipefail
n="$1"
err=$(mktemp)
trap 'rm -f "$err"' EXIT
if res=$(gh pr view "$n" --repo "$REPO" --json state,reviews \
  --jq '[.state, ([.reviews[].state] | any(. == "APPROVED"))] | @tsv' 2>"$err"); then
  printf '%s\t%s\n' "$n" "$res"
else
  echo "good-prs: 'gh pr view $n' failed: $(tr '\n' ' ' <"$err")" >&2
  exit 255
fi
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
      a=$(awk -F'\t' -v n="$n" '$1==n{print $2"\t"$3}' "$WORK/appr.tsv")
      state=$(echo "$a" | cut -f1); approved=$(echo "$a" | cut -f2)
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
      [ "$approved" = "true" ] && av="yes" || av="-"
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$g" "$gl" "$n" "$av" "$auth" "$title"
    done | sort -t$'\t' -k1,1n -k3,3n \
  | while IFS=$'\t' read -r g gl n av auth title; do
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
echo "_Legend: GREEN = fully green / merge-ready · FAILED = only \`CH Inc sync\` failed · INPROG = only \`CH Inc sync\` still running · NOSYNC = no \`CH Inc sync\` check, everything else green (e.g. release-branch backports). Approved = approved by someone at least once. Merged/closed PRs excluded._"
echo
echo "## 1. Your own PRs (authored by \`$ME\`)"
echo
echo "| sync | approved | PR | title |"
echo "|------|:--:|----|-------|"
emit "$WORK/authored.meta" 0 ""
echo
echo "## 2. Assigned to you, authored by others (excluding: ${excl_csv})"
echo
echo "| sync | approved | PR | author | title |"
echo "|------|:--:|----|--------|-------|"
emit "$WORK/assigned.meta" 1 "$ME,$excl_csv"
for a in "${TRACK_AUTHORS[@]}"; do
  echo
  echo "## 3. PRs by \`$a\` (regardless of assignee)"
  echo
  echo "| sync | approved | PR | title |"
  echo "|------|:--:|----|-------|"
  grep -P "^\d+\t$a\t" "$WORK/tracked.meta" > "$WORK/one_author.meta" || true
  emit "$WORK/one_author.meta" 0 ""
done
