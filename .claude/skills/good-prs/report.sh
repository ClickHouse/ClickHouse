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
#   - the "CH Inc sync" state: GREEN (also passed), FAILED, or INPROG (still running)
#   - whether it was APPROVED by anyone at least once
# Already-merged / closed PRs are excluded.
#
# Usage:
#   report.sh [tracked-author ...]      # default tracked author: groeneai
#
set -euo pipefail

TRACK_AUTHORS=("groeneai")
if [ "$#" -gt 0 ]; then TRACK_AUTHORS=("$@"); fi

ME=$(gh api user --jq .login)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# ---- helper invoked per-PR by xargs: classify the checks --------------------
# Output: "<num>\t<CH-Inc-sync-state>\t<#other-non-green>"
cat > "$WORK/classify.sh" <<'SH'
#!/usr/bin/env bash
n="$1"
json=$(gh pr checks "$n" --json name,state 2>/dev/null)
if [ -z "$json" ] || [ "$json" = "[]" ]; then echo -e "$n\tNO_CHECKS\t99"; exit 0; fi
echo "$json" | jq -r --arg n "$n" '
  (map(select(.name=="CH Inc sync")) | .[0].state // "ABSENT") as $ch
  | (map(select(.name!="CH Inc sync" and .name!="PR" and .name!="Mergeable Check"
               and .name!="A Sync (only for tests)"))) as $others
  | ($others | map(select(.state!="SUCCESS" and .state!="SKIPPED" and .state!="NEUTRAL")) | length) as $nbad
  | "\($n)\t\($ch)\t\($nbad)"'
SH
chmod +x "$WORK/classify.sh"

# ---- helper invoked per-PR by xargs: state + ever-approved -----------------
# Output: "<num>\t<STATE>\t<true|false>"
cat > "$WORK/approval.sh" <<'SH'
#!/usr/bin/env bash
n="$1"
res=$(gh pr view "$n" --json state,reviews \
  --jq '[.state, ([.reviews[].state] | any(. == "APPROVED"))] | @tsv' 2>/dev/null)
[ -z "$res" ] && res="ERR\tfalse"
printf '%s\t%s\n' "$n" "$res"
SH
chmod +x "$WORK/approval.sh"

# ---- gather metadata (number, author, title) for each query group ----------
gh pr list --author "$ME"   --state open --limit 1000 --json number,title,author \
  --jq '.[]|"\(.number)\t\(.author.login)\t\(.title)"' > "$WORK/authored.meta"
gh pr list --assignee "$ME" --state open --limit 1000 --json number,title,author \
  --jq '.[]|"\(.number)\t\(.author.login)\t\(.title)"' > "$WORK/assigned.meta"
: > "$WORK/tracked.meta"
for a in "${TRACK_AUTHORS[@]}"; do
  gh pr list --author "$a" --state open --limit 1000 --json number,title,author \
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
xargs -r -P 12 -n 1 "$WORK/classify.sh" < "$WORK/all_nums.txt" > "$WORK/checks.tsv" 2>/dev/null
# Only PRs that match the criterion need an approval/state lookup
awk -F'\t' '$3==0 && ($2=="SUCCESS"||$2=="FAILURE"||$2=="PENDING"||$2=="IN_PROGRESS"||$2=="ABSENT"){print $1}' \
  "$WORK/checks.tsv" | sort -un > "$WORK/match_nums.txt"
xargs -r -P 12 -n 1 "$WORK/approval.sh" < "$WORK/match_nums.txt" > "$WORK/appr.tsv" 2>/dev/null

# ---- render one markdown section -------------------------------------------
# args: <meta-file> <show-author-col:0|1> <exclude-authors-csv>
emit() {
  local meta="$1" showauth="$2" excl="$3"
  awk -F'\t' '$3==0 && ($2=="SUCCESS"||$2=="FAILURE"||$2=="PENDING"||$2=="IN_PROGRESS"||$2=="ABSENT"){print $1"\t"$2}' \
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
        SUCCESS) g=3; gl="GREEN";; FAILURE) g=1; gl="FAILED";;
        PENDING|IN_PROGRESS) g=2; gl="INPROG";; ABSENT) g=4; gl="NOSYNC";;
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
