#!/usr/bin/env bash
#
# Compare two TPC-H benchmark runs side-by-side.
#
# Usage:
#   ./compare.sh <run_dir_a> <run_dir_b>
#
# Compares best hot times, shows speedups, and flags result mismatches.

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <run_dir_a> <run_dir_b>" >&2
    exit 1
fi

DIR_A="$1"
DIR_B="$2"

if [[ ! -f "${DIR_A}/summary.tsv" ]] || [[ ! -f "${DIR_B}/summary.tsv" ]]; then
    echo "Error: summary.tsv not found in one of the directories" >&2
    exit 1
fi

TAG_A="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['tag'])" "${DIR_A}/metadata.json" 2>/dev/null)" || TAG_A="$(basename "$DIR_A")"
TAG_B="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['tag'])" "${DIR_B}/metadata.json" 2>/dev/null)" || TAG_B="$(basename "$DIR_B")"

echo "=== TPC-H Comparison ==="
echo "  A: ${TAG_A} (${DIR_A})"
echo "  B: ${TAG_B} (${DIR_B})"
echo ""

# Extract best hot time per query from a summary.tsv file.
best_hot_times() {
    awk -F'\t' '
    NR == 1 { next }
    $2 == "hot" {
        q = $1; t = $4 + 0
        if (!(q in best) || t < best[q]) best[q] = t
    }
    END {
        PROCINFO["sorted_in"] = "@ind_str_asc"
        for (q in best) print q "\t" best[q]
    }' "$1"
}

# Build associative arrays.
declare -A TIMES_A TIMES_B
while IFS=$'\t' read -r q t; do
    TIMES_A["$q"]="$t"
done < <(best_hot_times "${DIR_A}/summary.tsv")

while IFS=$'\t' read -r q t; do
    TIMES_B["$q"]="$t"
done < <(best_hot_times "${DIR_B}/summary.tsv")

# Collect all query names.
ALL_QUERIES=()
for q in "${!TIMES_A[@]}" "${!TIMES_B[@]}"; do
    ALL_QUERIES+=("$q")
done
IFS=$'\n' ALL_QUERIES=($(printf '%s\n' "${ALL_QUERIES[@]}" | sort -u))

printf "%-6s  %10s  %10s  %10s  %s\n" "Query" "$TAG_A" "$TAG_B" "Ratio" "Change"
printf "%-6s  %10s  %10s  %10s  %s\n" "-----" "----------" "----------" "----------" "------"

TOTAL_A=0
TOTAL_B=0
FASTER=0
SLOWER=0

for q in "${ALL_QUERIES[@]}"; do
    ta="${TIMES_A[$q]:-}"
    tb="${TIMES_B[$q]:-}"

    if [[ -z "$ta" ]]; then
        printf "%-6s  %10s  %10.3f  %10s  %s\n" "$q" "-" "$tb" "-" "only in B"
        continue
    fi
    if [[ -z "$tb" ]]; then
        printf "%-6s  %10.3f  %10s  %10s  %s\n" "$q" "$ta" "-" "-" "only in A"
        continue
    fi

    RATIO="$(echo "scale=2; ${tb} / ${ta}" | bc 2>/dev/null)" || RATIO="?"
    TOTAL_A="$(echo "$TOTAL_A + $ta" | bc)"
    TOTAL_B="$(echo "$TOTAL_B + $tb" | bc)"

    if (( $(echo "$tb < $ta * 0.95" | bc -l) )); then
        CHANGE="FASTER"
        FASTER=$((FASTER + 1))
    elif (( $(echo "$tb > $ta * 1.05" | bc -l) )); then
        CHANGE="SLOWER"
        SLOWER=$((SLOWER + 1))
    else
        CHANGE="~same"
    fi

    printf "%-6s  %10.3f  %10.3f  %10sx  %s\n" "$q" "$ta" "$tb" "$RATIO" "$CHANGE"
done

TOTAL_RATIO="$(echo "scale=2; ${TOTAL_B} / ${TOTAL_A}" | bc 2>/dev/null)" || TOTAL_RATIO="?"
printf "%-6s  %10.3f  %10.3f  %10sx\n" "TOTAL" "$TOTAL_A" "$TOTAL_B" "$TOTAL_RATIO"

echo ""
echo "B is faster in ${FASTER} queries, slower in ${SLOWER} queries"

# ---------- Check result correctness ----------
echo ""
echo "=== Result Comparison ==="
MISMATCHES=0
for q in "${ALL_QUERIES[@]}"; do
    QN="${q#Q}"
    # Compare first hot run results.
    FILE_A="${DIR_A}/results/q${QN}_hot_1.tsv"
    FILE_B="${DIR_B}/results/q${QN}_hot_1.tsv"
    if [[ -f "$FILE_A" ]] && [[ -f "$FILE_B" ]]; then
        if ! diff -q "$FILE_A" "$FILE_B" > /dev/null 2>&1; then
            echo "  ${q}: MISMATCH"
            diff --unified=3 "$FILE_A" "$FILE_B" | head -20
            MISMATCHES=$((MISMATCHES + 1))
        fi
    fi
done

if [[ "$MISMATCHES" -eq 0 ]]; then
    echo "  All query results match."
else
    echo ""
    echo "  ${MISMATCHES} queries produced different results!"
fi
