#!/bin/bash

set -e

echo "Merging LLVM coverage files..."

# Debug: List available llvm tools
echo "Available LLVM tools:"
command -v llvm-profdata-21 || echo "llvm-profdata-21: not found"
command -v llvm-cov-21 || echo "llvm-cov-21: not found"
command -v llvm-profdata || echo "llvm-profdata: not found"
command -v llvm-cov || echo "llvm-cov: not found"

# Auto-detect available LLVM tools
if [ -z "$LLVM_PROFDATA" ]; then
  for ver in 21 20 19 18 17 16 ""; do
    if command -v "llvm-profdata${ver:+-$ver}" &> /dev/null; then
      LLVM_PROFDATA="llvm-profdata${ver:+-$ver}"
      break
    fi
  done
fi

if [ -z "$LLVM_COV" ]; then
  for ver in 21 20 19 18 17 16 ""; do
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

# Merge profdata files from all jobs (skip corrupted files with -failure-mode=warn)
echo "Merging profdata files..."

# Artifacts are downloaded to ci/tmp by the CI framework
cd ci/tmp || { echo "ERROR: ci/tmp directory not found"; exit 1; }

# List available profdata files for debugging
echo "Available profdata files in $(pwd):"
ls -lh *.profdata 2>/dev/null || echo "No profdata files found"

echo "Checking for binaries..."
ls -lh clickhouse unit_tests_dbms 2>/dev/null || echo "Warning: Some binaries not found"

# Make binaries executable
chmod +x clickhouse unit_tests_dbms 2>/dev/null || true

MERGE_OUTPUT=$("$LLVM_PROFDATA" merge -sparse -failure-mode=warn *.profdata -o merged.profdata 2>&1)
MERGE_EXIT_CODE=$?

# Log corrupted files
CORRUPTED_COUNT=$(echo "$MERGE_OUTPUT" | grep -c "invalid instrumentation profile\|file header is corrupt" || true)
if [ "$CORRUPTED_COUNT" -gt 0 ]; then
    echo "WARNING: Found $CORRUPTED_COUNT corrupted profdata files:"
    echo "$MERGE_OUTPUT" | grep "invalid instrumentation profile\|file header is corrupt" || true
fi

if [ $MERGE_EXIT_CODE -eq 0 ] && [ -f merged.profdata ]; then
    echo "Successfully merged coverage data to merged.profdata"
else
    echo "ERROR: Failed to merge coverage files"
    exit 1
fi

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
        -ignore-filename-regex='contrib|_gtest_|\.pb\.|\.generated\.' \
        -skip-expansions \
        > llvm_coverage.info

sed -i "s|^SF:ci/tmp/build/|SF:$WORKSPACE_PATH/|" "llvm_coverage.info"
rm -rf ./coverage_html/*

echo "Generating HTML report..."
genhtml --version
genhtml "llvm_coverage.info" \
    --output-directory "llvm_coverage_html_report" \
    --verbose \
    --title "ClickHouse Coverage Report" \
    --legend \
    --demangle-cpp \
    --branch-coverage \
    --function-coverage \
    --num-spaces 4 \
    --sort-tables \
    --hierarchical \
    --css-file $WORKSPACE_PATH/ci/jobs/scripts/css.css \
    --no-function-coverage \
    --prefix $WORKSPACE_PATH \
    --ignore-errors inconsistent \
    --ignore-errors category \
    --ignore-errors corrupt \
    --ignore-errors unsupported \
    --ignore-errors source \
    --ignore-errors branch \
    --ignore-errors range

