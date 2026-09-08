#!/bin/bash

# SQLancer nightly job.
#
# Runs the ClickHouse SQLancer provider (https://github.com/ClickHouse/sqlancer,
# branch `main`) against a server started from this workflow's ClickHouse binary.
#
# Console output is deliberately terse: one progress line every ~5 minutes plus a
# summary at the end. Everything else is written to files that are attached to
# the job report:
#   sqlancer.out                   full fuzzer stdout+stderr
#   sqlancer-build.log             maven build of the sqlancer checkout
#   clickhouse-server.log[.err]    server stdout / stderr
#   failures/<databaseN>.log       ONE FILE PER FINDING: the exact
#                                  CREATE/INSERT/.../SELECT sequence that
#                                  reproduces it plus the reporting oracle's
#                                  stack trace
#   failures/analysis.txt          all findings deduplicated into distinct
#                                  failures, loudest first, plus a per-finding
#                                  index (see ci/jobs/scripts/sqlancer_failures.py)
#   failures/server-fatal.log      sanitizer report / `<Fatal>` server messages
# The report gets one row per *distinct* failure ("TLPWhere / AssertionError at
# ComparatorHelper.assumeResultSetsAreEqual:127 (x7)") listing every occurrence
# and carrying that failure's reproducer logs, so a 5h run that hit the same bug
# 40 times reads as one row - not 40, and not one wall of interleaved
# multi-threaded text.
#
# The job is marked FAILED whenever anything was found: a reproducer, a
# sanitizer/fatal server message, a dead or unreachable server, a non-zero
# sqlancer exit code, or a run that produced no statistics at all. Both signals
# are needed for that: the result file's status drives the HTML report, while the
# GitHub job conclusion comes from this script's *exit code* (`res = run_code == 0`
# in `ci/praktika/runner.py`), so a finding also has to exit non-zero.

set -eu
set -o pipefail

# Capture the job start timestamp so the result file (written by the EXIT trap)
# can report a real `start_time` and `duration`. Praktika's CIDB inserter
# rejects `null` `start_time` (it calls `datetime.utcfromtimestamp(start_time)`
# which fails with `'NoneType' object cannot be interpreted as an integer`).
JOB_START_TIME=$(date +%s)

REPO_DIR=$(readlink -f .)
TMP_PATH=$(readlink -f ./ci/tmp/)
OUTPUT_PATH="$TMP_PATH/sqlancer_output"
FAILURES_PATH="$OUTPUT_PATH/failures"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"

# Praktika reads the job result from `./ci/tmp/result_<normalized_job_name>.json`,
# where the normalization matches `Utils.normalize_string` in `ci/praktika/utils.py`.
# The `name` INSIDE that file must be the job name too: the workflow report is
# updated with `Result.update_sub_result`, which matches the job's placeholder
# entry by name, and silently keeps the placeholder when nothing matches - which
# is why this job used to show up in the report with no status, no sub-results and
# no attached logs at all. `JOB_NAME` is not propagated into the docker container,
# so read it from the serialized environment file Praktika writes before the job.
JOB_META=$(python3 -c '
import sys
sys.path.insert(0, ".")
from ci.praktika._environment import _Environment
from ci.praktika.utils import Utils
name = _Environment.get().JOB_NAME
print(name)
print(Utils.normalize_string(name))
')
JOB_NAME="$(printf '%s\n' "$JOB_META" | sed -n 1p)"
NORMALIZED_JOB_NAME="$(printf '%s\n' "$JOB_META" | sed -n 2p)"
RESULT_FILE="$TMP_PATH/result_${NORMALIZED_JOB_NAME}.json"

# Start from an empty output directory. In CI the workflow wipes `ci/tmp` before
# every job, but a local `praktika run` reuses it, and a leftover
# `aborted-on-finding-flood` sentinel or a stale `sqlancer.out` would be read as
# this run's - and attached to this run's report.
rm -rf "$OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH" "$FAILURES_PATH"

# Properly JSON-escape a string using python3, outputting only the inner
# content (without surrounding quotes) so callers can embed it in "...".
json_escape() {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
}

# Praktika uploads every file listed in a (sub-)result to S3 and links it in the
# report (see `_ResultS3.upload_result_files_to_s3`). A 5h run produces a large
# statement log, so compress anything above 10 MB; smaller files stay plain text
# so the report renders them inline. Echoes the path to attach.
prepare_attachment() {
    local f="$1" size
    if [ ! -f "$f" ]; then
        [ -f "$f.gz" ] && printf '%s' "$f.gz" && return 0
        return 1
    fi
    size=$(stat -c%s "$f" 2>/dev/null || echo 0)
    if [ "$size" -gt $((10 * 1024 * 1024)) ] && gzip -f "$f"; then
        f="$f.gz"
    fi
    [ -f "$f" ] || return 1
    printf '%s' "$f"
}

files_json_for() {
    local out="" f attach
    for f in "$@"; do
        attach="$(prepare_attachment "$f")" || continue
        [ -n "$out" ] && out+=", "
        out+="\"$attach\""
    done
    printf '%s' "$out"
}

# Sub-results are stored tab-separated (name, status, info) with a parallel array
# holding each row's attached files. Tabs never occur in the collapsed one-line
# infos built below, and a tab inside `info` would be harmless anyway since
# `read` puts the whole remainder into the last field.
add_test_result() {
    local name="$1" status="$2" info="$3"
    shift 3
    name="$(printf '%s' "$name" | tr '\n\t' '  ')"
    info="$(printf '%s' "$info" | tr '\n\t' '  ')"
    TEST_RESULTS+=("$(printf '%s\t%s\t%s' "$name" "$status" "$info")")
    TEST_RESULT_FILES+=("$*")
}

# Write result on any exit to ensure logs are always uploaded as artifacts
write_result() {
    # Praktika's Result.Status uses uppercase tokens (OK / FAIL / ERROR);
    # any other value is rendered as "completed but unknown" in the report.
    local status="${OVERALL_STATUS:-ERROR}"
    local info
    if [ -n "${RESULT_INFO:-}" ]; then
        info="$RESULT_INFO"
    elif [ "$status" = "OK" ]; then
        info=""
    elif [ "$status" = "FAIL" ]; then
        info="SQLancer found something - see the rows below"
    else
        info="Script terminated unexpectedly"
    fi

    # The deduplicated failure families come from `sqlancer_failures.py` as a
    # ready JSON fragment (comma-separated objects); they go first so the report
    # opens with the analysis.
    local results_json=""
    if [ -s "${SUBRESULTS_FRAGMENT:-}" ]; then
        results_json="$(cat "$SUBRESULTS_FRAGMENT")"
    fi
    if [ ${#TEST_RESULTS[@]} -gt 0 ]; then
        local i test_name test_status test_info test_files_json
        for i in "${!TEST_RESULTS[@]}"; do
            IFS=$'\t' read -r test_name test_status test_info <<< "${TEST_RESULTS[i]}"
            # shellcheck disable=SC2086
            test_files_json="$(files_json_for ${TEST_RESULT_FILES[i]})"
            if [ -n "$results_json" ]; then
                results_json+=","
            fi
            results_json+=$(printf '\n    {"name": "%s", "status": "%s", "files": [%s], "info": "%s"}' \
                "$(json_escape "$test_name")" "$test_status" "$test_files_json" "$(json_escape "$test_info")")
        done
    fi

    local candidate_files=(
        "$OUTPUT_PATH/clickhouse-server.log"
        "$OUTPUT_PATH/clickhouse-server.log.err"
    )
    candidate_files+=("${ATTACHED_FILES_ARRAY[@]+"${ATTACHED_FILES_ARRAY[@]}"}")
    local files_json
    files_json="$(files_json_for "${candidate_files[@]}")"

    local escaped_overall_info
    escaped_overall_info="$(json_escape "$info")"

    local job_duration=$(( $(date +%s) - JOB_START_TIME ))

    printf '{\n' > "$RESULT_FILE"
    printf '  "name": "%s",\n' "$(json_escape "$JOB_NAME")" >> "$RESULT_FILE"
    printf '  "status": "%s",\n' "$status" >> "$RESULT_FILE"
    printf '  "start_time": %d,\n' "$JOB_START_TIME" >> "$RESULT_FILE"
    printf '  "duration": %d,\n' "$job_duration" >> "$RESULT_FILE"
    printf '  "results": [%s\n  ],\n' "$results_json" >> "$RESULT_FILE"
    printf '  "files": [%s],\n' "$files_json" >> "$RESULT_FILE"
    printf '  "info": "%s"\n' "$escaped_overall_info" >> "$RESULT_FILE"
    printf '}\n' >> "$RESULT_FILE"
}

# Initialize variables used by the trap handler
TEST_RESULTS=()
TEST_RESULT_FILES=()
ATTACHED_FILES_ARRAY=()
OVERALL_STATUS="ERROR"
RESULT_INFO=""
SUBRESULTS_FRAGMENT=""

trap write_result EXIT

if [[ ! -f "$CLICKHOUSE_BIN" ]]; then
    RESULT_INFO="$CLICKHOUSE_BIN does not exist"
    echo "$RESULT_INFO" >&2
    exit 1
fi

chmod +x "$CLICKHOUSE_BIN"
"$CLICKHOUSE_BIN" local --version

# ---------------------------------------------------------------------------
# SQLancer checkout
# ---------------------------------------------------------------------------
# The image bakes a build of `ClickHouse/sqlancer@main` (see
# `ci/docker/sqlancer-test/Dockerfile`), but that image is only rebuilt when its
# digest - the contents of the docker directory - changes, so the baked build
# freezes at whatever `main` was when the Dockerfile was last touched. Re-clone
# and rebuild `main` here so every nightly run actually fuzzes current `main`;
# the baked build is the fallback (and its warm maven repo makes this rebuild
# take ~1 minute). The resolved commit is reported in the job info so a finding
# stays attributable.
SQLANCER_REPO="${SQLANCER_REPO:-https://github.com/ClickHouse/sqlancer}"
SQLANCER_REF="${SQLANCER_REF:-main}"
SQLANCER_BAKED_DIR=/sqlancer/sqlancer-main
SQLANCER_RUN_DIR=/sqlancer/checkout
SQLANCER_BUILD_LOG="$OUTPUT_PATH/sqlancer-build.log"
MAVEN_REPO=/sqlancer/.m2
# The job container runs as `--user $(id -u):$(id -g)`, an id that has no entry
# in the image's /etc/passwd, so `$HOME` may be unset or unwritable. Point it at
# the world-writable /sqlancer tree so git and maven have somewhere to scribble.
export HOME=/sqlancer

# Exit code says what went wrong, because the cases mean different things:
#   1 - nothing was built for a reason outside this repository: the clone failed,
#       or maven could not reach/resolve its dependencies. Transient - warn and
#       fall back.
#   2 - `main` was fetched and maven ran, but the code does not compile or no jar
#       came out. A real regression in the repository this job exists to fuzz, and
#       the reason the run below is not testing what the job promises.
# Maven reports both through a failed `package`, so the log decides. The patterns
# are maven's own transport/resolution wording; anything else counts as broken code.
MAVEN_TRANSIENT_PATTERN='Could not resolve dependencies|Could not transfer artifact|Failed to read artifact descriptor|Non-resolvable|could not be resolved|Connection (timed out|reset)|Read timed out|Connect to .* failed|status code: 5[0-9][0-9]|Unknown host|Network is unreachable'
build_sqlancer() {
    local ref="$1" dest="$2"
    rm -rf "$dest"
    git clone --quiet --depth 1 --branch "$ref" "$SQLANCER_REPO" "$dest" || return 1
    # Remember what we fetched before anything else can fail: on a broken `main`
    # this is the only place the offending revision is known, and the run has to
    # stay attributable exactly there.
    SQLANCER_FETCHED_COMMIT="$(git -c safe.directory='*' -C "$dest" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
    if ! (
        cd "$dest" &&
        mvn --no-transfer-progress -B package \
            -Dmaven.test.skip=true -Djacoco.skip=true \
            -Dmaven.repo.local="$MAVEN_REPO"
    ) > "$SQLANCER_BUILD_LOG" 2>&1; then
        if grep -qE "$MAVEN_TRANSIENT_PATTERN" "$SQLANCER_BUILD_LOG"; then
            return 1
        fi
        return 2
    fi
    compgen -G "$dest/target/sqlancer-*.jar" > /dev/null || return 2
}

SQLANCER_DIR="$SQLANCER_BAKED_DIR"
BUILD_WARNING=""
SQLANCER_FETCHED_COMMIT=""
echo "=== Building sqlancer from $SQLANCER_REPO @ $SQLANCER_REF ==="
if [ "${SQLANCER_BUILD_AT_RUNTIME:-1}" = "1" ]; then
    BUILD_RC=0
    build_sqlancer "$SQLANCER_REF" "$SQLANCER_RUN_DIR" || BUILD_RC=$?
    if [ "$BUILD_RC" = "0" ]; then
        SQLANCER_DIR="$SQLANCER_RUN_DIR"
    elif [ "$BUILD_RC" = "1" ]; then
        BUILD_WARNING="could not fetch or resolve dependencies for $SQLANCER_REF (network?), fell back to the image's build"
        echo "WARNING: $BUILD_WARNING" >&2
    else
        BUILD_WARNING="$SQLANCER_REF @ ${SQLANCER_FETCHED_COMMIT:-unknown} does not build, fell back to the image's build - this run does NOT test current $SQLANCER_REF"
        echo "ERROR: $BUILD_WARNING; see $SQLANCER_BUILD_LOG" >&2
        # Fuzz on with the baked build - some coverage beats none - but fail the
        # job: a broken `main` is exactly what this job must not hide.
        add_test_result "sqlancer $SQLANCER_REF build" ERROR "$BUILD_WARNING" "$SQLANCER_BUILD_LOG"
    fi
fi
if [ -f "$SQLANCER_BUILD_LOG" ]; then
    ATTACHED_FILES_ARRAY+=("$SQLANCER_BUILD_LOG")
fi

# `safe.directory`: the image's clone is owned by root while the job runs as the
# runner's uid, which git otherwise refuses to read ("dubious ownership").
SQLANCER_COMMIT="$(git -c safe.directory='*' -C "$SQLANCER_DIR" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
echo "Using sqlancer checkout [$SQLANCER_DIR] at commit [$SQLANCER_COMMIT]"

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------
# Apply the server-side config overrides shipped with the SQLancer provider
# (`.claude/clickhouse-config/`): drop the file logger to `warning`, remove the
# heavy `system.*_log` tables, and pin the profile settings the oracles depend on
# (`async_insert=0`, `alter_sync=2`, `mutations_sync=2`) so an INSERT/ALTER/
# mutation is visible to the next read in the same oracle iteration. The server
# runs without `--config-file`, so it uses the binary's embedded `config.xml` and
# merges any `config.d/*.xml` relative to its working directory (see
# `ConfigProcessor::loadConfig`); starting it from `$SERVER_DIR` is what makes the
# overrides take effect. The embedded config's `users_config` points at
# `config.xml` itself, so `<profiles>` overrides placed in `config.d/` reach the
# default profile (verified) -- unlike a packaged server, this does not need a
# separate `users.d/`.
SERVER_DIR="$TMP_PATH/server"
mkdir -p "$SERVER_DIR/config.d"
cp "$SQLANCER_DIR"/.claude/clickhouse-config/*.xml "$SERVER_DIR/config.d/"

# Fuzzing means ~2% of statements fail on purpose, and ClickHouse logs every one
# of them at error level *with a full stack trace* - measured 10 MB of stderr per
# 20 minutes (~150 MB over a 5h run) of pure noise. There is no switch for the
# trace alone (it is baked into the message by `Exception::getStackTraceString`),
# so raise the level instead. `fatal` keeps exactly what this job looks at: the
# `<Fatal>` lines of a crash, and sanitizer reports, which the runtime writes
# straight to stderr and not through the logger. Set SQLANCER_SERVER_LOG_LEVEL to
# `warning` for a debugging run. The `zz_` prefix sorts this last in config.d so
# it overrides the provider's own `log_level.xml`.
cat > "$SERVER_DIR/config.d/zz_ci_log_level.xml" <<XML
<clickhouse>
    <logger>
        <level>${SQLANCER_SERVER_LOG_LEVEL:-fatal}</level>
    </logger>
</clickhouse>
XML

# Several oracles read through a cluster named `default` (`cluster('default', ...)`
# and `ENGINE = Distributed('default', ...)`), which the embedded config does not
# define -- it carries no `remote_servers` at all, so those reads fail with
# `CLUSTER_DOESNT_EXIST`. 127.0.0.1 is treated as local, unlike 127.0.0.{2..255},
# so the shard is served in process rather than over a loopback connection.
cat > "$SERVER_DIR/config.d/zz_ci_default_cluster.xml" <<'XML'
<clickhouse>
    <remote_servers>
        <default>
            <shard>
                <internal_replication>false</internal_replication>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </default>
    </remote_servers>
</clickhouse>
XML

echo "=== Starting ClickHouse server ==="
( cd "$SERVER_DIR" && exec "$CLICKHOUSE_BIN" server -P "$PID_FILE" ) 1>"$OUTPUT_PATH/clickhouse-server.log" 2>"$OUTPUT_PATH/clickhouse-server.log.err" &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd "$SQLANCER_DIR"

# Run all oracles in a single invocation bounded by one wall-clock timeout,
# matching the upstream `.claude/run-sqlancer.sh --oracles all` reference run.
# The provider uses the `com.clickhouse:client-v2` transport and pins its own
# per-request settings (`max_execution_time`, `wait_end_of_query`,
# `max_result_rows`, ...).
#
# Derive the oracle list from the provider's own curated `ALL_ORACLES` (its
# `--oracles all` set) instead of hardcoding it here: the list changes between
# sqlancer revisions (oracles are added, and noisy ones such as `RowPolicy` are
# dropped), so reading it from the checkout keeps it in sync with whatever commit
# is being run. Fail closed if it cannot be parsed -- an empty `--oracle` would
# otherwise make sqlancer error out cryptically.
#
# `--random-session-settings` is intentionally NOT passed: it is rejected in
# combination with the `SEMR`/`SEMRMulti` oracles (which toggle session settings
# themselves), and those oracles are part of the curated list and already
# provide setting-differential coverage.
TIMEOUT="${SQLANCER_TIMEOUT_SECONDS:-18000}"
NUM_THREADS=10
ORACLES=$(grep -E '^ALL_ORACLES=' "$SQLANCER_DIR/.claude/run-sqlancer.sh" | head -1 | cut -d'"' -f2)
if [ -z "$ORACLES" ]; then
    RESULT_INFO="Could not parse ALL_ORACLES from the sqlancer run script"
    echo "$RESULT_INFO" >&2
    exit 1
fi

# Oracles excluded from the curated run. `TextIndexDirectRead` asserts that a
# text-index `hasToken` lookup (`use_skip_indexes` on) matches a full scan, but
# it currently dominates the run with known index-vs-scan divergences (several
# tokenizers, cf. ClickHouse#107186) that would keep the job perpetually red on
# the same finding. Drop it until that divergence class is resolved upstream.
EXCLUDED_ORACLES="TextIndexDirectRead"
for excluded in $EXCLUDED_ORACLES; do
    ORACLES=$(printf '%s' "$ORACLES" | tr ',' '\n' | grep -vxF "$excluded" | paste -sd, -)
done
if [ -z "$ORACLES" ]; then
    RESULT_INFO="Oracle list is empty after applying exclusions"
    echo "$RESULT_INFO" >&2
    exit 1
fi
echo "Oracles ($(printf '%s' "$ORACLES" | tr ',' '\n' | grep -c .)): $ORACLES"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
OUTPUT_FILE="$OUTPUT_PATH/sqlancer.out"
ATTACHED_FILES_ARRAY+=("$OUTPUT_FILE")

if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) != 'Ok.' ]]; then
    add_test_result "SQLancer" ERROR "Server is not responding before the run"
    OVERALL_STATUS="FAIL"
    RESULT_INFO="Server is not responding before the SQLancer run"
    exit 1
fi

echo "=== Running SQLancer for ${TIMEOUT}s with $NUM_THREADS threads ==="
echo "(console shows one progress line per ~5 min; full output goes to sqlancer.out)"

# Finding-flood watchdog. One dominant bug can produce hundreds of reproducers -
# every one of them also kills its worker and orphans a database, which
# eventually drags the server down - so the rest of the budget is spent
# re-finding the same thing. Stop once the run has surfaced enough DISTINCT
# failures to fill a triage session; everything found so far is kept and
# reported.
MAX_DISTINCT_FAILURES="${SQLANCER_MAX_DISTINCT_FAILURES:-50}"
ABORT_FLAG="$OUTPUT_PATH/aborted-on-finding-flood"
watch_finding_flood() {
    local counts distinct
    while sleep 60; do
        pgrep -f 'target/sqlancer-.*\.jar' > /dev/null || return 0
        counts="$(python3 "$REPO_DIR/ci/jobs/scripts/sqlancer_failures.py" \
            --logs-dir "$SQLANCER_DIR/logs/clickhouse" --out-dir "$FAILURES_PATH" --dry-run 2>/dev/null || true)"
        distinct="$(printf '%s' "$counts" | cut -f2)"
        case "$distinct" in ''|*[!0-9]*) continue ;; esac
        if [ "$distinct" -ge "$MAX_DISTINCT_FAILURES" ]; then
            echo "$distinct" > "$ABORT_FLAG"
            echo "=== Stopping SQLancer: $distinct distinct failures reached (cap $MAX_DISTINCT_FAILURES) ==="
            pkill -f 'target/sqlancer-.*\.jar' || true
            return 0
        fi
    done
}
watch_finding_flood &
FLOOD_WATCHDOG_PID=$!

JAVA_EXIT=0
# Everything sqlancer prints is kept in `$OUTPUT_FILE`; the console gets only the
# start-up chatter and one progress line per ~5 min (sqlancer prints one every
# 5s). Without this filter a 5h run buries the job log under ~3600 progress lines
# plus, for every finding, the whole reproducer state dump that SQLancer's
# `AlsoWriteToConsoleFileWriter` mirrors to stderr.
java -jar target/sqlancer-*.jar \
    --num-threads "$NUM_THREADS" \
    --num-tries 999999 \
    --timeout-seconds "$TIMEOUT" \
    --use-connection-test false \
    --print-progress-summary true \
    --host 127.0.0.1 --port 8123 \
    --username default --password "" \
    clickhouse --oracle "$ORACLES" 2>&1 | tee "$OUTPUT_FILE" | awk '
    /Executed [0-9]+ queries/ { progress++; if (progress % 60 == 0) { print; fflush() } next }
    { other++; if (other <= 100) { print; fflush() } }
' || JAVA_EXIT=${PIPESTATUS[0]}

kill "$FLOOD_WATCHDOG_PID" 2>/dev/null || true
echo "=== SQLancer finished (exit code $JAVA_EXIT) ==="

# ---------------------------------------------------------------------------
# Findings
# ---------------------------------------------------------------------------
OVERALL_STATUS=OK
FAILURE_COUNT=0
FAILURE_SUMMARY="no findings"
# A pathological night could produce hundreds of reproducers; cap the uploads.
# Every finding still appears in `analysis.txt`, and each family's row lists all
# of its occurrences by database name, so nothing is dropped silently.
MAX_ATTACHED_REPRODUCERS=50

# Collect, fingerprint and deduplicate the reproducer logs (one per finding, see
# the script's docstring) into failure families: one report row per distinct
# failure with the individual findings and their logs nested underneath, plus
# `failures/analysis.txt`. A 5h run hits the same bug many times, so the family
# view is what makes the result readable.
echo "=== Analysing findings ==="
SQLANCER_LOG_DIR="$SQLANCER_DIR/logs/clickhouse"
FAILURE_ANALYSIS="$(python3 "$REPO_DIR/ci/jobs/scripts/sqlancer_failures.py" \
    --logs-dir "$SQLANCER_LOG_DIR" \
    --out-dir "$FAILURES_PATH" \
    --max-files "$MAX_ATTACHED_REPRODUCERS" || true)"
IFS=$'\t' read -r FAILURE_COUNT FAILURE_FAMILIES FAILURE_SUMMARY <<< "$FAILURE_ANALYSIS"
if [ -z "${FAILURE_COUNT:-}" ]; then
    # The analysis itself failed. Never let that swallow a finding: count the raw
    # logs, attach them wholesale and report the analysis as broken.
    FAILURE_COUNT="$(find "$SQLANCER_LOG_DIR" -maxdepth 1 -name 'database*.log' ! -name '*-cur.log' -size +0 2>/dev/null | wc -l)"
    FAILURE_FAMILIES=0
    FAILURE_SUMMARY="failure analysis failed; $FAILURE_COUNT raw reproducer log(s)"
    raw_archive="$OUTPUT_PATH/sqlancer_reproducer_logs.tar"
    if tar -C "$(dirname "$SQLANCER_LOG_DIR")" -cf "$raw_archive" "$(basename "$SQLANCER_LOG_DIR")" 2>/dev/null; then
        add_test_result "Failure analysis" ERROR "sqlancer_failures.py failed; raw reproducer logs attached" "$raw_archive"
    else
        add_test_result "Failure analysis" ERROR "sqlancer_failures.py failed"
    fi
    echo "ERROR: $FAILURE_SUMMARY" >&2
else
    SUBRESULTS_FRAGMENT="$FAILURES_PATH/subresults.json"
fi
echo "$FAILURE_SUMMARY"
if [ -f "$ABORT_FLAG" ]; then
    add_test_result "SQLancer stopped early" FAIL \
        "stopped after $(cat "$ABORT_FLAG") distinct failures (cap $MAX_DISTINCT_FAILURES) - the remaining budget would only have re-found them"
    FAILURE_SUMMARY="$FAILURE_SUMMARY; stopped early at the distinct-failure cap"
fi
if [ "$FAILURE_COUNT" -gt 0 ] && [ -f "$FAILURES_PATH/analysis.txt" ]; then
    ATTACHED_FILES_ARRAY+=("$FAILURES_PATH/analysis.txt" "$FAILURES_PATH/findings.json")
    sed -n '/^x[0-9]/,/^Per-finding index/{/^Per-finding index/!p}' "$FAILURES_PATH/analysis.txt" | head -n 60
fi

# Sanitizer reports and `<Fatal>` messages are a finding on their own, even when
# no oracle noticed anything. Shared with the SQLancer++ job, which runs against
# the same builds.
# shellcheck source=./scripts/sqlancer_server_errors.sh
. "$REPO_DIR/ci/jobs/scripts/sqlancer_server_errors.sh"
SERVER_ERROR_REPORT="$FAILURES_PATH/server-fatal.log"
SERVER_ERROR_FINGERPRINT=""
if server_error_line="$(scan_server_errors \
        "$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err" "$SERVER_ERROR_REPORT")"; then
    # Name the row after the report's identity - kind plus the frame it happened
    # in - rather than "sanitizer report": that name is the CI DB test name and the
    # alert fingerprint, so two different sanitizer bugs must not collapse into one
    # row, and the same one must not re-alert every night.
    SERVER_ERROR_FINGERPRINT="Sanitizer/Fatal: $(server_error_signature "$SERVER_ERROR_REPORT")"
    add_test_result "$SERVER_ERROR_FINGERPRINT" FAIL "$server_error_line" "$SERVER_ERROR_REPORT"
    # Fold it into the run summary too: it is a finding with no oracle reproducer,
    # so the counts from the analysis do not include it, and a sanitizer-only run
    # would otherwise be summarized as "no findings".
    if [ "$FAILURE_SUMMARY" = "no findings" ]; then
        FAILURE_SUMMARY="1 server-log finding"
    else
        FAILURE_SUMMARY="$FAILURE_SUMMARY; 1 server-log finding"
    fi
    echo " - server log finding: $server_error_line"
fi

if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) != 'Ok.' ]]; then
    add_test_result "Server is alive after the run" FAIL "Server died during the SQLancer run"
    echo " - server died during the run"
fi

if [ "$JAVA_EXIT" -ne 0 ] && [ "$FAILURE_COUNT" -eq 0 ]; then
    # A non-zero exit with no reproducer file means sqlancer itself failed to run
    # (bad arguments, OOM, jar problem) - that is an infrastructure error, not a
    # ClickHouse finding.
    add_test_result "SQLancer run" ERROR "SQLancer exited with code $JAVA_EXIT and left no reproducer log" \
        "$OUTPUT_FILE"
    echo " - sqlancer exited with code $JAVA_EXIT without leaving a reproducer log"
fi

# Belt and braces: a finding whose reproducer log could not be read still shows
# up as an AssertionError in the fuzzer output.
ASSERTION_COUNT="$(grep -c 'AssertionError' "$OUTPUT_FILE" || true)"
if [ "${ASSERTION_COUNT:-0}" -gt 0 ] && [ "$FAILURE_COUNT" -eq 0 ]; then
    add_test_result "SQLancer assertions" FAIL \
        "$ASSERTION_COUNT AssertionError line(s) in the fuzzer output but no reproducer log was written" \
        "$OUTPUT_FILE"
    echo " - $ASSERTION_COUNT AssertionError line(s) without a reproducer log"
fi

# Guard against a silently broken run: with `--print-progress-summary true`
# sqlancer always prints the statistics block when it stops.
QUERY_STATS="$(awk '/Overall execution statistics/,0' "$OUTPUT_FILE" | grep -m1 'queries' | tr -s ' ' | sed -e 's/^ //' -e 's/ $//' || true)"
if [ -z "$QUERY_STATS" ] && [ ! -f "$ABORT_FLAG" ]; then
    add_test_result "SQLancer statistics" ERROR "SQLancer produced no execution statistics" "$OUTPUT_FILE"
    echo " - no execution statistics in the fuzzer output"
fi

# The console filter above hides ordinary fuzzer output, so show the tail when
# sqlancer itself misbehaved - that is where "jar not found", a bad option or an
# OOM kill shows up. Not when findings explain the non-zero exit: sqlancer exits
# with its error code whenever a worker died on an assertion, and the analysis
# above already says what happened.
if [ ! -f "$ABORT_FLAG" ] && { [ -z "$QUERY_STATS" ] || { [ "$JAVA_EXIT" -ne 0 ] && [ "$FAILURE_COUNT" -eq 0 ]; }; }; then
    echo "=== last 30 lines of sqlancer.out ==="
    tail -n 30 "$OUTPUT_FILE" || true
fi

if [ "$FAILURE_COUNT" -gt 0 ]; then
    OVERALL_STATUS="FAIL"
fi
for entry in "${TEST_RESULTS[@]+"${TEST_RESULTS[@]}"}"; do
    case "$entry" in
        *$'\t'FAIL$'\t'*|*$'\t'ERROR$'\t'*) OVERALL_STATUS="FAIL" ;;
    esac
done
if [ "$OVERALL_STATUS" = "OK" ]; then
    add_test_result "SQLancer" OK ""
fi

# Say which revision actually ran. On a fallback the fetched SHA is not the one
# that produced these findings, and reporting it would misattribute them.
if [ "$SQLANCER_DIR" = "$SQLANCER_RUN_DIR" ]; then
    SQLANCER_PROVENANCE="sqlancer $SQLANCER_REF @ $SQLANCER_COMMIT"
else
    SQLANCER_PROVENANCE="sqlancer image build @ $SQLANCER_COMMIT (NOT $SQLANCER_REF @ ${SQLANCER_FETCHED_COMMIT:-not fetched})"
fi
RESULT_INFO="$SQLANCER_PROVENANCE; ${QUERY_STATS:-no statistics}; $FAILURE_SUMMARY"
if [ -n "$BUILD_WARNING" ]; then
    RESULT_INFO="$RESULT_INFO; WARNING: $BUILD_WARNING"
fi
echo "=== Summary: $OVERALL_STATUS - $RESULT_INFO ==="

# Alert on NEW findings only: `sqlancer_notify.py` diffs this run's fingerprints
# against the ones CIDB has already seen for this job and posts to Slack when
# something is new. A no-op without SLACK_WEBHOOK_CORE_QA in the environment, and
# never fatal - a notification problem must not change the job's verdict.
if [ -f "$FAILURES_PATH/findings.json" ]; then
    python3 "$REPO_DIR/ci/jobs/scripts/sqlancer_notify.py" \
        --findings "$FAILURES_PATH/findings.json" \
        --job-name "$JOB_NAME" \
        --info "$RESULT_INFO" \
        --extra-failure "$SERVER_ERROR_FINGERPRINT" \
        --extra-failure-message "$server_error_line" || echo "WARNING: new-finding notification failed"
fi

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
if [ -f "$PID_FILE" ]; then
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    # Validate the PID before sending a signal: it must be numeric, the process
    # must still exist, and `/proc/<pid>/exe` must resolve to the exact
    # `clickhouse` binary that this job started. This protects against
    # signalling an unrelated process on a shared runner if the PID file is
    # stale or the PID has been reused by another `clickhouse` process.
    expected_exe="$(readlink -f "$CLICKHOUSE_BIN" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && [ -e "/proc/$pid/exe" ]; then
        proc_exe="$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)"
        if [ -n "$expected_exe" ] && [ "$proc_exe" = "$expected_exe" ]; then
            kill "$pid" || true
        else
            echo "Warning: PID $pid in $PID_FILE does not belong to this job's clickhouse binary (exe=[$proc_exe], expected=[$expected_exe]); not signalling"
        fi
    else
        echo "Warning: PID file $PID_FILE contains invalid or stale PID [$pid]; not signalling"
    fi
else
    echo "Warning: PID file not found at $PID_FILE"
fi
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done

# Praktika derives the GitHub job conclusion from this exit code, so a finding
# has to exit non-zero to turn the nightly red.
[ "$OVERALL_STATUS" = "OK" ]
