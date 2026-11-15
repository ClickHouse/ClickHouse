#!/usr/bin/env bash
# rebuild.sh — fetch CI staging data and run the full Keeper-stress analysis pipeline.
#
# Usage:
#   rebuild.sh [WORK_DIR] [TS_FILTER] [TS_FILTER_END]
#
# Args:
#   WORK_DIR       Where to drop staging/ and outputs. Default: ./tmp/keeper_stress_skill
#   TS_FILTER      Lower-bound ts (ISO yyyy-mm-dd[Thh:mm:ss], inclusive). Default: 2026-03-25
#   TS_FILTER_END  Upper-bound ts (ISO yyyy-mm-dd[Thh:mm:ss], exclusive). Default: 9999-12-31
#                  (i.e. unbounded — equivalent to today's behavior). Pass an explicit
#                  end date for "between A and B" date-range analyses; otherwise newer
#                  nightlies will keep accumulating into the result.
#
# The skill home is determined automatically from the script location.
set -euo pipefail

SKILL_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${1:-./tmp/keeper_stress_skill}"
TS_FILTER="${2:-2026-03-25}"
TS_FILTER_END="${3:-9999-12-31}"

WORK_DIR="$(realpath -m "$WORK_DIR")"
mkdir -p "$WORK_DIR/staging" "$WORK_DIR/queries"

# Pass both bounds to the Python pipeline (build_pr_nightly_map.py and
# compute_deltas.py read these for the in-window vs out-of-window split).
export KEEPER_SKILL_THRESHOLD="$TS_FILTER"
export KEEPER_SKILL_THRESHOLD_END="$TS_FILTER_END"

# Copy scripts and queries into WORK_DIR so the Python scripts find their
# staging/ siblings (the scripts use Path(__file__).parent as ROOT).
cp -f "$SKILL_HOME"/scripts/*.py        "$WORK_DIR/"
cp -f "$SKILL_HOME"/queries/*.sql       "$WORK_DIR/queries/"

echo "== Skill home: $SKILL_HOME"
echo "== Work dir:   $WORK_DIR"
echo "== ts window:  >= $TS_FILTER  AND  < $TS_FILTER_END"
echo

PLAY_URL='https://play.clickhouse.com/?user=play'
fetch() {
  local sql_file="$1"
  local out_file="$2"
  local sql_text
  # Substitute both {{TS_FILTER}} (lower) and {{TS_FILTER_END}} (upper) placeholders.
  sql_text=$(sed -e "s|{{TS_FILTER}}|$TS_FILTER|g" -e "s|{{TS_FILTER_END}}|$TS_FILTER_END|g" "$sql_file")
  # --fail-with-body: exit non-zero on HTTP >=400 but still write the body, so
  # the failing staging file shows the server's error message. Combined with
  # `set -e` above, any 4xx/5xx response halts the pipeline immediately
  # instead of feeding garbage into the downstream Python pipeline.
  curl -sS --fail-with-body -G "$PLAY_URL" --data-urlencode "query=$sql_text" > "$out_file"
}

echo "== 1/8 Pull bench_summary"
fetch "$WORK_DIR/queries/01_bench_summary.sql" "$WORK_DIR/staging/bench_summary.tsv"
echo "== 2/8 Pull prom_rates"
fetch "$WORK_DIR/queries/02_prom_rates.sql"    "$WORK_DIR/staging/prom_rates.tsv"
echo "== 3/8 Pull prom_gauges"
fetch "$WORK_DIR/queries/03_prom_gauges.sql"   "$WORK_DIR/staging/prom_gauges.tsv"
echo "== 4/8 Pull mntr"
fetch "$WORK_DIR/queries/04_mntr.sql"          "$WORK_DIR/staging/mntr.tsv"
echo "== 5/8 Pull container"
fetch "$WORK_DIR/queries/05_container.sql"     "$WORK_DIR/staging/container.tsv"
echo "== 6/8 Pull pr_branches"
# In PR mode (caller supplied pr_meta.tsv) this fetch is required — fail fast.
# In date-range mode it's unused; tolerate failure so an outage on this single
# query doesn't block a window analysis that doesn't need it.
if [[ -f "$WORK_DIR/../pr_meta.tsv" ]]; then
  fetch "$WORK_DIR/queries/06_pr_branch.sql"   "$WORK_DIR/staging/pr_branches.tsv"
else
  fetch "$WORK_DIR/queries/06_pr_branch.sql"   "$WORK_DIR/staging/pr_branches.tsv" || true
fi

echo
echo "== Build merged_metrics.tsv"
( cd "$WORK_DIR" && python3 keeper_stress.py merge )

# pr_meta.tsv must be supplied by caller in WORK_DIR's parent (../pr_meta.tsv)
# if PR-set analysis is requested. The scripts gracefully handle its absence.
if [[ -f "$WORK_DIR/../pr_meta.tsv" ]]; then
  echo "== Build PR -> nightly map"
  ( cd "$WORK_DIR" && python3 keeper_stress.py prmap )
  echo "== Compute per-PR + per-nightly deltas"
  ( cd "$WORK_DIR" && python3 keeper_stress.py deltas )
  echo "== Build per_pr_metrics_long.tsv"
  ( cd "$WORK_DIR" && python3 keeper_stress.py prmetrics )
  echo "== Build PR-branch isolated effects"
  ( cd "$WORK_DIR" && python3 keeper_stress.py prisol )
fi

echo "== Build cumulative_gains.tsv"
( cd "$WORK_DIR" && python3 keeper_stress.py cumulative )

echo
echo "Outputs in $WORK_DIR:"
ls -la "$WORK_DIR"/*.tsv "$WORK_DIR"/*.md 2>/dev/null || true

# On-demand subcommands (not part of the default pipeline):
#   python3 keeper_stress.py diff <shaA> <shaB>  — per-metric Δ between two
#                                                  arbitrary commits
#   python3 keeper_stress.py noise               — per-(scenario, backend, metric)
#                                                  median / stddev / cv / p95
