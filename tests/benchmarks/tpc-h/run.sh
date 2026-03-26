#!/usr/bin/env bash
#
# TPC-H benchmark runner for ClickHouse.
#
# Runs all 22 TPC-H queries, collects execution times (cold + hot), query plans,
# and optimizer traces.  Results are stored in a timestamped directory for later
# comparison between runs.
#
# Usage:
#   ./run.sh [OPTIONS]
#
# Options:
#   --host HOST              Server address (default: localhost)
#   --port PORT              Native protocol port (default: 9000)
#   --user USER              Username (default: default)
#   --password PASS          Password (default: empty)
#   --client PATH            Path to clickhouse binary (auto-detected if omitted)
#   --secure                 Use TLS for client connection
#   --estimates              Add estimates=1 to EXPLAIN output
#   --database DB            Database containing TPC-H tables (default: default)
#   --settings 'K=V,...'     Extra ClickHouse settings, comma-separated
#   --cost-weights JSON      Cascades cost config JSON, e.g. '{"network_weight":10}'
#   --cluster-size N         Number of nodes (informational tag, also set as
#                            make_distributed_plan_cluster_size if >1)
#   --cold-runs N            Number of cold runs (default: 1)
#   --hot-runs N             Number of hot runs (default: 3)
#   --queries LIST           Comma-separated query numbers to run (default: all)
#   --output-dir DIR         Base output directory (default: ./results)
#   --tag LABEL              Human-readable tag for this run
#   --help                   Show this help
#
# Examples:
#   # Local execution, default settings
#   ./run.sh --database tpch
#
#   # Distributed with Cascades optimizer
#   ./run.sh --host cluster-gw --database tpch \
#       --settings 'make_distributed_plan=1,enable_cascades_optimizer=1' \
#       --cluster-size 20 --tag distributed_cascades
#
#   # Compare local vs distributed
#   ./run.sh --database tpch --tag local
#   ./run.sh --database tpch \
#       --settings 'make_distributed_plan=1,enable_cascades_optimizer=1' \
#       --cluster-size 20 --tag distributed

set -euo pipefail

# ---------- defaults ----------
HOST="localhost"
PORT="9000"
USER="default"
PASSWORD=""
DATABASE="default"
EXTRA_SETTINGS=""
COST_WEIGHTS=""
CLUSTER_SIZE=1
COLD_RUNS=1
HOT_RUNS=3
QUERIES=""
OUTPUT_BASE="./results"
TAG=""
SECURE=0
ESTIMATES=0
CLIENT_BIN=""
PORT_EXPLICIT=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERIES_DIR="${SCRIPT_DIR}/queries"

# ---------- parse args ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)         HOST="$2";         shift 2 ;;
        --port)         PORT="$2"; PORT_EXPLICIT=1; shift 2 ;;
        --user)         USER="$2";         shift 2 ;;
        --password)     PASSWORD="$2";     shift 2 ;;
        --database)     DATABASE="$2";     shift 2 ;;
        --settings)     EXTRA_SETTINGS="$2"; shift 2 ;;
        --cost-weights) COST_WEIGHTS="$2"; shift 2 ;;
        --cluster-size) CLUSTER_SIZE="$2"; shift 2 ;;
        --cold-runs)    COLD_RUNS="$2";    shift 2 ;;
        --hot-runs)     HOT_RUNS="$2";     shift 2 ;;
        --queries)      QUERIES="$2";      shift 2 ;;
        --output-dir)   OUTPUT_BASE="$2";  shift 2 ;;
        --tag)          TAG="$2";          shift 2 ;;
        --secure)       SECURE=1;          shift ;;
        --estimates)    ESTIMATES=1;       shift ;;
        --client)       CLIENT_BIN="$2";   shift 2 ;;
        --help)
            sed -n '2,/^$/{ s/^# \?//; p }' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ---------- apply defaults that depend on other flags ----------
if [[ "$SECURE" -eq 1 ]] && [[ "$PORT_EXPLICIT" -eq 0 ]]; then
    PORT="9440"
fi

# ---------- build query list ----------
if [[ -n "$QUERIES" ]]; then
    IFS=',' read -ra QUERY_NUMS <<< "$QUERIES"
else
    QUERY_NUMS=($(seq -w 1 22))
fi

# ---------- resolve client binary ----------
if [[ -z "$CLIENT_BIN" ]]; then
    # Try to find clickhouse binary: PATH first, then well-known build dirs.
    if command -v clickhouse &>/dev/null; then
        CLIENT_BIN="clickhouse"
    else
        REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
        for candidate in \
            "${REPO_ROOT}/build/programs/clickhouse" \
            "${REPO_ROOT}/build/RelWithDebInfo/programs/clickhouse" \
            "${REPO_ROOT}/build_Debug/programs/clickhouse" \
            "${REPO_ROOT}/build_debug/programs/clickhouse" \
            "${REPO_ROOT}/build_asan/programs/clickhouse" \
            "${REPO_ROOT}/build_tsan/programs/clickhouse"; do
            if [[ -x "$candidate" ]]; then
                CLIENT_BIN="$candidate"
                break
            fi
        done
    fi
    if [[ -z "$CLIENT_BIN" ]]; then
        echo "Error: clickhouse binary not found. Use --client /path/to/clickhouse" >&2
        exit 1
    fi
fi

# ---------- build client command ----------
CLIENT_BASE=("$CLIENT_BIN" client
    --host "$HOST"
    --port "$PORT"
    --user "$USER"
    --database "$DATABASE"
    --output_format_pretty_color 0
)
if [[ -n "$PASSWORD" ]]; then
    CLIENT_BASE+=(--password "$PASSWORD")
fi
if [[ "$SECURE" -eq 1 ]]; then
    CLIENT_BASE+=(--secure)
fi

# ---------- build settings arrays ----------
# Always-on settings for reproducibility.
SETTINGS_ARGS=(
    --join_use_nulls=1
    --send_logs_level=trace
    --allow_experimental_analyzer=1
)
SETTINGS_DESC="join_use_nulls=1, send_logs_level=trace, allow_experimental_analyzer=1"

# Cluster size is passed via SET param__internal_cascades_cluster_node_count
# in the SET_PREFIX block below.
# Cost weights and cluster size use query-level parameters (SET param__internal_*)
# that can't be passed as --key=value CLI flags.
# Build a SET prefix that is prepended to every query instead.
SET_PREFIX=""
if [[ -n "$COST_WEIGHTS" ]]; then
    SET_PREFIX+="SET param__internal_cascades_cost_config='${COST_WEIGHTS}';"
    SETTINGS_DESC+=", cascades_cost_weights=${COST_WEIGHTS}"
fi
if [[ "$CLUSTER_SIZE" -gt 1 ]]; then
    SET_PREFIX+="SET param__internal_cascades_cluster_node_count=${CLUSTER_SIZE};"
    SETTINGS_DESC+=", cascades_cluster_node_count=${CLUSTER_SIZE}"
fi
if [[ -n "$EXTRA_SETTINGS" ]]; then
    IFS=',' read -ra EXTRA_PAIRS <<< "$EXTRA_SETTINGS"
    for pair in "${EXTRA_PAIRS[@]}"; do
        pair="${pair# }"   # strip leading space
        pair="${pair% }"   # strip trailing space
        SETTINGS_ARGS+=("--${pair}")
    done
    SETTINGS_DESC+=", ${EXTRA_SETTINGS}"
fi

# ---------- create output directory ----------
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_LABEL="${TIMESTAMP}"
if [[ -n "$TAG" ]]; then
    RUN_LABEL="${TIMESTAMP}_${TAG}"
fi
RUN_DIR="${OUTPUT_BASE}/${RUN_LABEL}"
mkdir -p "${RUN_DIR}"/{plans,logs,times,results}

# ---------- save run metadata ----------
cat > "${RUN_DIR}/metadata.json" <<METADATA
{
    "timestamp": "${TIMESTAMP}",
    "tag": "${TAG}",
    "host": "${HOST}",
    "port": ${PORT},
    "user": "${USER}",
    "database": "${DATABASE}",
    "extra_settings": "${EXTRA_SETTINGS}",
    "cost_weights": "${COST_WEIGHTS}",
    "cluster_size": ${CLUSTER_SIZE},
    "cold_runs": ${COLD_RUNS},
    "hot_runs": ${HOT_RUNS},
    "queries": "$(IFS=,; echo "${QUERY_NUMS[*]}")",
    "settings": "${SETTINGS_DESC}"
}
METADATA

# ---------- save command line to repeat the run ----------
{
    echo "#!/usr/bin/env bash"
    echo "# Command to repeat this run.  Generated by run.sh on ${TIMESTAMP}."
    echo "cd '$(pwd)' && \\"
    printf "  '%s'" "$0"
    [[ -n "$CLIENT_BIN" ]]      && printf " \\\\\n  --client '%s'" "$CLIENT_BIN"
    printf " \\\\\n  --host '%s'" "$HOST"
    printf " \\\\\n  --port '%s'" "$PORT"
    printf " \\\\\n  --user '%s'" "$USER"
    [[ -n "$PASSWORD" ]]        && printf " \\\\\n  --password '%s'" "$PASSWORD"
    [[ "$SECURE" -eq 1 ]]       && printf " \\\\\n  --secure"
    [[ "$ESTIMATES" -eq 1 ]]    && printf " \\\\\n  --estimates"
    printf " \\\\\n  --database '%s'" "$DATABASE"
    [[ -n "$EXTRA_SETTINGS" ]]  && printf " \\\\\n  --settings '%s'" "$EXTRA_SETTINGS"
    [[ -n "$COST_WEIGHTS" ]]    && printf " \\\\\n  --cost-weights '%s'" "$COST_WEIGHTS"
    [[ "$CLUSTER_SIZE" -gt 1 ]] && printf " \\\\\n  --cluster-size %d" "$CLUSTER_SIZE"
    printf " \\\\\n  --cold-runs %d" "$COLD_RUNS"
    printf " \\\\\n  --hot-runs %d" "$HOT_RUNS"
    [[ -n "$QUERIES" ]]         && printf " \\\\\n  --queries '%s'" "$QUERIES"
    printf " \\\\\n  --output-dir '%s'" "$OUTPUT_BASE"
    [[ -n "$TAG" ]]             && printf " \\\\\n  --tag '%s'" "$TAG"
    echo ""
} > "${RUN_DIR}/rerun.sh"
chmod +x "${RUN_DIR}/rerun.sh"

echo "=== TPC-H Benchmark Run: ${RUN_LABEL} ==="
echo "    Host:         ${HOST}:${PORT}"
echo "    Database:     ${DATABASE}"
echo "    Cluster size: ${CLUSTER_SIZE}"
echo "    Runs:         ${COLD_RUNS} cold + ${HOT_RUNS} hot"
echo "    Output:       ${RUN_DIR}"
echo "    Settings:     ${SETTINGS_DESC}"
echo ""

# ---------- helper: report query status after execution ----------
# Prints elapsed time + row count, or error if the query failed.
# Sets QUERY_ERROR to the error message (empty on success).
report_query_status() {
    local log_file="$1"
    local result_file="$2"
    local elapsed="$3"

    QUERY_ERROR=""
    local err_msg
    err_msg="$(grep -oP 'Code: \d+\. DB::Exception: \K[^(]+' "$log_file" 2>/dev/null | head -1 || true)"
    if [[ -z "$err_msg" ]]; then
        err_msg="$(grep -oP 'DB::Exception:.*?(?=\. \(|, Stack)' "$log_file" 2>/dev/null | head -1 || true)"
    fi

    if [[ -n "$err_msg" ]]; then
        QUERY_ERROR="$err_msg"
        echo "${elapsed}s ERROR: ${err_msg}"
    else
        local result_rows
        result_rows="$(wc -l < "$result_file")"
        echo "${elapsed}s (${result_rows} rows)"
    fi
}

# ---------- helper: run a utility command (no SET_PREFIX, no trace logs) ----------
run_util() {
    local sql="$1"
    shift
    "${CLIENT_BASE[@]}" "$@" <<< "$sql"
}

# ---------- helper: run a query via client ----------
run_query() {
    local sql="$1"
    shift
    "${CLIENT_BASE[@]}" "${SETTINGS_ARGS[@]}" "$@" <<< "${SET_PREFIX}${sql}"
}

# ---------- helper: run EXPLAIN with test-level logs (Cascades task traces) ----------
run_query_explain() {
    local sql="$1"
    shift
    # Build settings with send_logs_level overridden to test.
    local -a explain_args=()
    for arg in "${SETTINGS_ARGS[@]}"; do
        [[ "$arg" == --send_logs_level=* ]] && continue
        explain_args+=("$arg")
    done
    explain_args+=(--send_logs_level=test)
    "${CLIENT_BASE[@]}" "${explain_args[@]}" "$@" <<< "${SET_PREFIX}${sql}"
}

# ---------- helper: run query from file ----------
run_query_file() {
    local file="$1"
    shift
    if [[ -n "$SET_PREFIX" ]]; then
        # Prepend SET to the query file content via stdin.
        { echo "$SET_PREFIX"; cat "$file"; } | "${CLIENT_BASE[@]}" "${SETTINGS_ARGS[@]}" "$@"
    else
        "${CLIENT_BASE[@]}" "${SETTINGS_ARGS[@]}" "$@" --queries-file "$file"
    fi
}

# ---------- collect server version ----------
echo -n "Server version: "
run_util "SELECT version()" 2>/dev/null | tee "${RUN_DIR}/server_version.txt" || true
echo ""

# ---------- summary CSV header ----------
SUMMARY_FILE="${RUN_DIR}/summary.tsv"
printf "query\trun_type\trun_num\telapsed_sec\tread_rows\tread_bytes\tresult_rows\terror\n" > "$SUMMARY_FILE"

# ---------- main loop ----------
for QN in "${QUERY_NUMS[@]}"; do
    # Zero-pad to 2 digits.
    QN_PAD="$(printf '%02d' "$((10#$QN))")"
    QUERY_FILE="${QUERIES_DIR}/query_${QN_PAD}.sql"

    if [[ ! -f "$QUERY_FILE" ]]; then
        echo "WARNING: ${QUERY_FILE} not found, skipping Q${QN_PAD}"
        continue
    fi

    echo "--- Q${QN_PAD} ---"

    QUERY_SQL="$(cat "$QUERY_FILE")"

    # ---- Collect EXPLAIN plan ----
    # Q15 uses CREATE VIEW / DROP VIEW, so EXPLAIN won't work for it directly.
    # For multi-statement queries, extract the main SELECT.
    # Collect trace-level logs alongside the plan — they contain join reordering
    # decisions and Cascades optimization details.
    EXPLAIN_PREFIX="EXPLAIN PLAN keep_logical_steps = 1"
    if [[ "$ESTIMATES" -eq 1 ]]; then
        EXPLAIN_PREFIX+=", estimates = 1"
    fi

    PLAN_FILE="${RUN_DIR}/plans/q${QN_PAD}.txt"
    PLAN_LOG_FILE="${RUN_DIR}/plans/q${QN_PAD}.log"
    if echo "$QUERY_SQL" | grep -qi "CREATE VIEW"; then
        # For Q15: run the CREATE VIEW, then EXPLAIN the SELECT, then DROP VIEW.
        VIEW_SQL="$(echo "$QUERY_SQL" | sed -n '/^CREATE VIEW/,/;/p')"
        MAIN_SELECT="$(echo "$QUERY_SQL" | sed -n '/^SELECT/,/;/p' | head -n -2)"
        DROP_SQL="$(echo "$QUERY_SQL" | grep -i "^DROP VIEW")"
        run_query "$VIEW_SQL" 2>/dev/null || true
        run_query_explain "${EXPLAIN_PREFIX} ${MAIN_SELECT}" > "$PLAN_FILE" 2> "$PLAN_LOG_FILE" || true
        run_query "$DROP_SQL" 2>/dev/null || true
    else
        run_query_explain "${EXPLAIN_PREFIX} ${QUERY_SQL}" > "$PLAN_FILE" 2> "$PLAN_LOG_FILE" || true
    fi

    # Report EXPLAIN errors — an empty plan file means the EXPLAIN failed.
    if [[ ! -s "$PLAN_FILE" ]]; then
        PLAN_ERROR="$(grep -oP 'Code: \d+\. DB::Exception: \K[^(]+' "$PLAN_LOG_FILE" 2>/dev/null | head -1 || true)"
        if [[ -z "$PLAN_ERROR" ]]; then
            PLAN_ERROR="$(grep -oP 'DB::Exception:.*?(?=\. \(|, Stack)' "$PLAN_LOG_FILE" 2>/dev/null | head -1 || true)"
        fi
        echo "  EXPLAIN FAILED: ${PLAN_ERROR:-empty plan, check ${PLAN_LOG_FILE}}"
    fi

    # ---- Drop filesystem caches before cold runs ----
    run_util "SYSTEM DROP FILESYSTEM CACHE" 2>/dev/null || true
    run_util "SYSTEM DROP MARK CACHE" 2>/dev/null || true
    run_util "SYSTEM DROP UNCOMPRESSED CACHE" 2>/dev/null || true
    run_util "SYSTEM DROP PAGE CACHE" 2>/dev/null || true

    # ---- Execute query: cold runs ----
    for ((r = 1; r <= COLD_RUNS; r++)); do
        echo -n "  cold run ${r}/${COLD_RUNS}... "

        LOG_FILE="${RUN_DIR}/logs/q${QN_PAD}_cold_${r}.log"
        TIME_FILE="${RUN_DIR}/times/q${QN_PAD}_cold_${r}.json"
        RESULT_FILE="${RUN_DIR}/results/q${QN_PAD}_cold_${r}.tsv"

        # Drop caches between cold runs.
        if [[ "$r" -gt 1 ]]; then
            run_util "SYSTEM DROP FILESYSTEM CACHE" 2>/dev/null || true
            run_util "SYSTEM DROP MARK CACHE" 2>/dev/null || true
            run_util "SYSTEM DROP UNCOMPRESSED CACHE" 2>/dev/null || true
            run_util "SYSTEM DROP PAGE CACHE" 2>/dev/null || true
        fi

        START_TS="$(date +%s%N)"
        run_query_file "$QUERY_FILE" --format TabSeparated \
            > "$RESULT_FILE" 2> "$LOG_FILE" || true
        END_TS="$(date +%s%N)"

        ELAPSED_NS=$(( END_TS - START_TS ))
        ELAPSED_SEC="$(echo "scale=3; ${ELAPSED_NS} / 1000000000" | bc)"

        # Extract stats from logs.
        READ_ROWS="$(grep -oP 'Read \K[0-9]+(?= rows)' "$LOG_FILE" | tail -1)" || READ_ROWS=""
        READ_BYTES="$(grep -oP 'Read [0-9]+ rows, \K[0-9.]+' "$LOG_FILE" | tail -1)" || READ_BYTES=""
        RESULT_ROWS="$(wc -l < "$RESULT_FILE")"

        report_query_status "$LOG_FILE" "$RESULT_FILE" "$ELAPSED_SEC"

        printf "%s\tcold\t%d\t%s\t%s\t%s\t%s\t%s\n" \
            "Q${QN_PAD}" "$r" "$ELAPSED_SEC" "$READ_ROWS" "$READ_BYTES" "$RESULT_ROWS" "$QUERY_ERROR" \
            >> "$SUMMARY_FILE"

        cat > "$TIME_FILE" <<TIMEJSON
{"query": "Q${QN_PAD}", "type": "cold", "run": ${r}, "elapsed_sec": ${ELAPSED_SEC}, "read_rows": "${READ_ROWS}", "result_rows": ${RESULT_ROWS}, "error": "${QUERY_ERROR}"}
TIMEJSON
    done

    # ---- Execute query: hot runs ----
    for ((r = 1; r <= HOT_RUNS; r++)); do
        echo -n "  hot  run ${r}/${HOT_RUNS}... "

        LOG_FILE="${RUN_DIR}/logs/q${QN_PAD}_hot_${r}.log"
        TIME_FILE="${RUN_DIR}/times/q${QN_PAD}_hot_${r}.json"
        RESULT_FILE="${RUN_DIR}/results/q${QN_PAD}_hot_${r}.tsv"

        START_TS="$(date +%s%N)"
        run_query_file "$QUERY_FILE" --format TabSeparated \
            > "$RESULT_FILE" 2> "$LOG_FILE" || true
        END_TS="$(date +%s%N)"

        ELAPSED_NS=$(( END_TS - START_TS ))
        ELAPSED_SEC="$(echo "scale=3; ${ELAPSED_NS} / 1000000000" | bc)"

        READ_ROWS="$(grep -oP 'Read \K[0-9]+(?= rows)' "$LOG_FILE" | tail -1)" || READ_ROWS=""
        READ_BYTES="$(grep -oP 'Read [0-9]+ rows, \K[0-9.]+' "$LOG_FILE" | tail -1)" || READ_BYTES=""
        RESULT_ROWS="$(wc -l < "$RESULT_FILE")"

        report_query_status "$LOG_FILE" "$RESULT_FILE" "$ELAPSED_SEC"

        printf "%s\thot\t%d\t%s\t%s\t%s\t%s\t%s\n" \
            "Q${QN_PAD}" "$r" "$ELAPSED_SEC" "$READ_ROWS" "$READ_BYTES" "$RESULT_ROWS" "$QUERY_ERROR" \
            >> "$SUMMARY_FILE"

        cat > "$TIME_FILE" <<TIMEJSON
{"query": "Q${QN_PAD}", "type": "hot", "run": ${r}, "elapsed_sec": ${ELAPSED_SEC}, "read_rows": "${READ_ROWS}", "result_rows": ${RESULT_ROWS}, "error": "${QUERY_ERROR}"}
TIMEJSON
    done
done

# ---------- extract optimizer traces from logs ----------
echo ""
echo "Extracting optimizer traces..."
TRACES_DIR="${RUN_DIR}/traces"
mkdir -p "$TRACES_DIR"

for LOG_FILE in "${RUN_DIR}"/logs/*.log; do
    BASE="$(basename "$LOG_FILE" .log)"
    TRACE_FILE="${TRACES_DIR}/${BASE}.trace"

    grep -E '(JoinOrderOptimizer|optimizeJoin|CascadesOptimizer|QueryPlanOptimizations.*join|DistributionEnforcer|SortingEnforcer|DefaultImplementation|Filter selectivity|estimate statistics|Optimizing join order|fillMemo|addGroup|buildBestPlan|Optimization took)' \
        "$LOG_FILE" > "$TRACE_FILE" 2>/dev/null || true

    # Remove empty trace files.
    [[ -s "$TRACE_FILE" ]] || rm -f "$TRACE_FILE"
done

# ---------- generate final report ----------
echo ""
echo "=== Summary ==="
echo ""
column -t -s $'\t' "$SUMMARY_FILE"

# Report queries that had errors during execution.
ERRORS="$(awk -F'\t' 'NR > 1 && $8 != "" { printf "  %-6s %s (%s #%s): %s\n", $1, $2, $3, $8 }' "$SUMMARY_FILE")"
if [[ -n "$ERRORS" ]]; then
    echo ""
    echo "=== Errors ==="
    echo ""
    echo "$ERRORS"
fi

# Compute per-query best hot time.
echo ""
echo "=== Best hot times ==="
echo ""
awk -F'\t' '
NR == 1 { next }
$2 == "hot" {
    q = $1
    t = $4 + 0
    e = $8
    if (e != "") {
        errors[q] = e
    } else {
        if (!(q in best) || t < best[q]) best[q] = t
    }
}
END {
    PROCINFO["sorted_in"] = "@ind_str_asc"
    total = 0
    failed = 0
    for (q in best) {
        printf "  %-6s %8.3fs\n", q, best[q]
        total += best[q]
    }
    for (q in errors) {
        if (!(q in best)) {
            printf "  %-6s    ERROR: %s\n", q, errors[q]
            failed++
        }
    }
    printf "  %-6s %8.3fs", "TOTAL", total
    if (failed > 0) printf "  (%d failed)", failed
    printf "\n"
}' "$SUMMARY_FILE"

echo ""
echo "Results saved to: ${RUN_DIR}"
echo "  plans/      - EXPLAIN plans + optimizer trace logs"
echo "  logs/       - Full server traces per execution"
echo "  traces/     - Extracted optimizer traces"
echo "  times/      - Per-run timing JSON"
echo "  results/    - Query result sets"
echo "  summary.tsv - All timings in one file"
echo "  rerun.sh    - Command to repeat this run"
