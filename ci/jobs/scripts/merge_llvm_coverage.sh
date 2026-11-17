#!/bin/bash

set -e

echo "Merging LLVM coverage files..."

# Debug: List available llvm tools
echo "Available LLVM tools:"
command -v llvm-profdata-22 || echo "llvm-profdata-22: not found"
command -v llvm-cov-22 || echo "llvm-cov-22: not found"
command -v llvm-profdata || echo "llvm-profdata: not found"
command -v llvm-cov || echo "llvm-cov: not found"

# Auto-detect available LLVM tools
if [ -z "$LLVM_PROFDATA" ]; then
  for ver in 22 21 20 19 18 17 16 ""; do
    if command -v "llvm-profdata${ver:+-$ver}" &> /dev/null; then
      LLVM_PROFDATA="llvm-profdata${ver:+-$ver}"
      break
    fi
  done
fi

if [ -z "$LLVM_COV" ]; then
  for ver in 22 21 20 19 18 17 16 ""; do
    if command -v "llvm-cov${ver:+-$ver}" &> /dev/null; then
      LLVM_COV="llvm-cov${ver:+-$ver}"
      break
    fi
  done
fi

echo "Using LLVM tools: LLVM_PROFDATA=$LLVM_PROFDATA, LLVM_COV=$LLVM_COV"

# Check if tools were found
if [ -z "$LLVM_PROFDATA" ]; then
  echo "ERROR: llvm-profdata not found in PATH"
  exit 1
fi

if [ -z "$LLVM_COV" ]; then
  echo "ERROR: llvm-cov not found in PATH"
  exit 1
fi

# This script runs in two phases, invoked as separate job steps:
#
#   merge  - merge the per-shard profiles. A failure here means "this run has no
#            complete measurement", which the job reports as SKIPPED, so the
#            status is passed back in a marker file rather than as an exit code.
#   report - llvm-cov export + genhtml. A failure here IS a tooling failure and
#            must stay RED, so this phase exits non-zero on error.
#
# Both phases run in one invocation ("all") for local use.
PHASE="${1:-all}"
case "$PHASE" in
  merge|report|all) ;;
  *) echo "ERROR: unknown phase '$PHASE' (expected merge, report or all)"; exit 1 ;;
esac

# Artifacts are downloaded to ci/tmp by the CI framework
cd ci/tmp || { echo "ERROR: ci/tmp directory not found"; exit 1; }

MERGE_STATUS_FILE="merge_profdata.status"

if [ "$PHASE" = "merge" ] || [ "$PHASE" = "all" ]; then
  echo "Merging profdata files..."

  # List available profdata files for debugging
  echo "Available profdata files in $(pwd):"
  ls -lh *.profdata 2>/dev/null || echo "No profdata files found"

  # The files to merge are named after their coverage artifacts and passed in
  # explicitly by the job, which has already verified the set is complete. An
  # unrestricted `*.profdata` glob would also pick up this script's own
  # merged.profdata from an earlier invocation, or a profile left under a stale
  # name, and fold it into the total with nothing to notice it.
  if [ -z "${MERGE_PROFDATA_FILES:-}" ]; then
    echo "ERROR: MERGE_PROFDATA_FILES is not set, refusing to merge an unbounded glob"
    exit 1
  fi
  read -ra PROFDATA_FILES <<< "${MERGE_PROFDATA_FILES}"
  echo "Merging ${#PROFDATA_FILES[@]} profdata file(s): ${PROFDATA_FILES[*]}"

  # --failure-mode=any makes the merge all-or-nothing: on any invalid input it
  # exits non-zero and writes no file, instead of silently dropping that input
  # and producing a total that is short by a whole shard.
  #
  # `set -e` is lifted around the merge deliberately - the point is to observe
  # its status, not to abort on it.
  rm -f "$MERGE_STATUS_FILE" merged.profdata
  set +e
  "$LLVM_PROFDATA" merge -sparse -failure-mode=any "${PROFDATA_FILES[@]}" -o merged.profdata 2>&1
  MERGE_EXIT_CODE=$?
  set -e

  if [ $MERGE_EXIT_CODE -eq 0 ] && [ -f merged.profdata ]; then
      echo "Successfully merged coverage data to merged.profdata"
      echo "ok" > "$MERGE_STATUS_FILE"
  else
      echo "ERROR: Failed to merge coverage files (exit code $MERGE_EXIT_CODE)"
      echo "failed" > "$MERGE_STATUS_FILE"
      # Deliberately exit 0 and produce no llvm_coverage.info: publishing
      # nothing is what lets the job report SKIPPED with a reason instead of
      # comparing a partial measurement. Never fabricate an .info.
      exit 0
  fi
fi

if [ "$PHASE" = "merge" ]; then
  exit 0
fi

if [ ! -f merged.profdata ]; then
  echo "merged.profdata is absent (the merge phase reported an incomplete measurement); nothing to report on"
  exit 0
fi

echo "Checking for binaries..."
ls -lh clickhouse unit_tests_dbms 2>/dev/null || echo "Warning: Some binaries not found"

# Make binaries executable
chmod +x clickhouse unit_tests_dbms 2>/dev/null || true

./clickhouse --version

# Generate HTML coverage report
echo "Generating coverage report..."
# The coverage data references paths like "ci/tmp/build/base/base/..."
# We created symlinks so those paths now resolve to actual source files
# Ignore contrib files (coverage is disabled for them)

# Detect workspace path - use WORKSPACE_PATH if set, otherwise try to detect
if [ -z "$WORKSPACE_PATH" ]; then
    # Go back to workspace root (we're in ci/tmp)
    WORKSPACE_PATH=$(cd ../.. && pwd)
fi

echo "Using workspace path: $WORKSPACE_PATH"

"$LLVM_COV" export   \
        -instr-profile=merged.profdata   \
        -object ./clickhouse   \
        -object ./unit_tests_dbms   \
        -format=lcov   \
        -path-equivalence=ci/tmp/build,$WORKSPACE_PATH \
        -ignore-filename-regex='contrib|_gtest_|\.pb\.|\.generated\.|/(QueryFuzzer|ThreadFuzzer|fuzzQuery|fuzzBits|StorageFuzzQuery|hasThreadFuzzer)\.(cpp|h)$|/fuzzers/' \
        -skip-expansions \
        > llvm_coverage.info

sed -i "s|^SF:ci/tmp/build/|SF:$WORKSPACE_PATH/|" "llvm_coverage.info"

echo "Deduplicating template instantiations..."
python3 "$WORKSPACE_PATH/ci/jobs/scripts/dedup_lcov_instantiations.py" llvm_coverage.info

rm -rf ./coverage_html/*

echo "Generating HTML report..."
genhtml --version

html_escape() { printf '%s' "$1" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g'; }
export -f html_escape

HEADER_TITLE="ClickHouse coverage report"
if [ -n "${PR_NUMBER}" ] && [ "${PR_NUMBER}" -gt 0 ]; then
  PR_URL="https://github.com/ClickHouse/ClickHouse/pull/${PR_NUMBER}"
  HEADER_TITLE="${HEADER_TITLE} &middot; <a href=\"${PR_URL}\">#${PR_NUMBER}</a>"
elif [ -n "${CURRENT_COMMIT}" ]; then
  COMMIT_URL="https://github.com/ClickHouse/ClickHouse/commit/${CURRENT_COMMIT}"
  COMMIT_SHORT="${CURRENT_COMMIT:0:12}"
  COMMIT_MSG=$(html_escape "$(git -C "$WORKSPACE_PATH" log -1 --format="%s" "${CURRENT_COMMIT}" 2>/dev/null | cut -c1-120 || true)")
  COMMIT_DATE=$(html_escape "$(git -C "$WORKSPACE_PATH" log -1 --format="%cs" "${CURRENT_COMMIT}" 2>/dev/null || true)")
  HEADER_TITLE="${HEADER_TITLE} &middot; <a href=\"${COMMIT_URL}\"><code>${COMMIT_SHORT}</code></a>"
  [ -n "${COMMIT_DATE}" ] && HEADER_TITLE="${HEADER_TITLE} &middot; ${COMMIT_DATE}"
  [ -n "${COMMIT_MSG}" ] && HEADER_TITLE="${HEADER_TITLE} &middot; ${COMMIT_MSG}"
fi

genhtml "llvm_coverage.info" \
    --header-title "${HEADER_TITLE}" \
    --title "branch=${BRANCH}, current_commit=${CURRENT_COMMIT}" \
    --baseline-title "base_branch=${BASE_BRANCH}, baseline_commit=${BASE_COMMIT}" \
    --output-directory "llvm_coverage_html_report" \
    --legend \
    --demangle-cpp \
    --branch-coverage \
    --function-coverage \
    --num-spaces 4 \
    --sort-tables \
    --hierarchical \
    --css-file $WORKSPACE_PATH/ci/jobs/scripts/css.css \
    --prefix $WORKSPACE_PATH \
    --ignore-errors inconsistent,inconsistent \
    --ignore-errors category \
    --ignore-errors corrupt \
    --ignore-errors unsupported \
    --ignore-errors source \
    --ignore-errors branch \
    --ignore-errors range,range \
    --ignore-errors count,count \
    --filter missing \
    --quiet 
