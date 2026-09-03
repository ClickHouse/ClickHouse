#!/bin/bash
# CI: only C/C++ source files are extracted for differential coverage analysis.
# Non-C++ changes (cmake, scripts, tests, configs) are intentionally skipped.
set -euo pipefail

# Validate required env vars
for var in PREV_30_COMMITS CURRENT_COMMIT BASE_COMMIT BRANCH BASE_BRANCH WORKSPACE_PATH; do
  if [ -z "${!var:-}" ]; then
    echo "ERROR: Required environment variable $var is not set"
    exit 1
  fi
done

cd ci/tmp

# Every exit-0 path names its outcome here, so an absent marker means the script
# died before reaching one. A stale marker must therefore not survive.
OUTCOME_MARKER="diff_outcome.txt"
rm -f "$OUTCOME_MARKER"

if [[ ! -f "llvm_coverage.info" ]]; then
  echo "ERROR: llvm_coverage.info not found"
  exit 1
fi

# Try to find .info file from S3, checking up to 30 ancestor commits
IFS=',' read -ra COMMITS <<< "${PREV_30_COMMITS}"

FOUND=0
FIRST_BASE_COMMIT=""
for TEST_COMMIT in "${COMMITS[@]}"; do
# Artifacts live under the normalized workflow name; the baseline coverage is
# produced by the MasterCI workflow:
#   REFs/master/<sha>/masterci/llvm_coverage/llvm_coverage.info
COVERAGE_URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${TEST_COMMIT}/masterci/llvm_coverage/llvm_coverage.info"
echo "Checking coverage file for commit ${TEST_COMMIT}..."
if wget --spider "${COVERAGE_URL}" 2>&1 | grep -q '200 OK'; then
echo "Found coverage file at ${COVERAGE_URL}"
wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
FIRST_BASE_COMMIT="${TEST_COMMIT}"
FOUND=1
break
fi
done

if [ $FOUND -eq 0 ]; then
  echo "ERROR: Could not find baseline coverage file after checking ${#COMMITS[@]} commits"
  exit 1
fi

# Note: base_llvm_coverage_{2..6}.info (extra older master baselines) are not
# downloaded anywhere. The slot loop below is a no-op unless something else
# populates those files.

export CURRENT_COMMIT
export BASE_COMMIT
export FIRST_BASE_COMMIT
export PR_NUMBER
export REPO_NAME

# Two separate ranges serve two different purposes:
#
# 1. changes.diff (for genhtml --diff-file): anchored at FIRST_BASE_COMMIT so
#    the diff maps the baseline's line numbers to current. The baseline .info was
#    produced at FIRST_BASE_COMMIT, so the "before" side of the diff must match.
#
# 2. changed_files (for pattern extraction and downstream analysis): anchored at
#    BASE_COMMIT (the actual PR merge base) so we only see files the PR itself
#    changed. Using FIRST_BASE_COMMIT here would pull in unrelated master commits
#    from the gap between FIRST_BASE_COMMIT and BASE_COMMIT — a src/Foo.cpp edit
#    from that gap would appear in patterns, set _diff_ran=True, and then cause
#    llvm_coverage_job.py to parse it into _changed_paths, flipping
#    _binary_unchanged=False and suppressing the newly-covered analysis even
#    though the PR binary is genuinely unchanged.
#
# `gh` reports a failure as a bare "gh: Not Found (HTTP 404)" naming no resource,
# so each endpoint is echoed before it is requested.
echo "Fetching diff: repos/ClickHouse/ClickHouse/compare/${FIRST_BASE_COMMIT}...${CURRENT_COMMIT}"
gh api \
  -H "Accept: application/vnd.github.v3.diff" \
  repos/ClickHouse/ClickHouse/compare/${FIRST_BASE_COMMIT}...${CURRENT_COMMIT} \
  > changes.diff
echo "Fetching changed files: repos/ClickHouse/ClickHouse/compare/${BASE_COMMIT}...${CURRENT_COMMIT}"
changed_files=$(gh api \
  repos/ClickHouse/ClickHouse/compare/${BASE_COMMIT}...${CURRENT_COMMIT} \
  --jq '.files[].filename'
)
echo "Changed files:"
echo "$changed_files"

if [ -z "$changed_files" ]; then
  echo "ERROR: No changed files reported by GitHub compare API"
  exit 1
fi

patterns=()
while IFS= read -r f; do
  # Only include C/C++ source files that can appear in lcov coverage data.
  # Skip contrib/ files — coverage is disabled for third-party code, so they
  # produce no records in the tracefile and cause lcov to fail with "(empty)".
  if [[ "$f" =~ \.(cpp|cc|cxx|c|h|hpp|hxx|hh)$ ]] && [[ ! "$f" =~ ^contrib/ ]]; then
    patterns+=("*$f")
  fi
done < <(echo "$changed_files")

if [ ${#patterns[@]} -eq 0 ]; then
  echo "No coverable C/C++ source files changed (contrib/ is excluded from coverage), skipping differential coverage report"
  echo "no_cpp_changes" > "$OUTCOME_MARKER"
  exit 0
fi

lcov --extract llvm_coverage.info "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt,empty,unsupported,unused \
  --quiet \
  -o current.changed.info

lcov --extract base_llvm_coverage.info "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt,empty,unsupported,unused \
  --quiet \
  -o baseline.changed.info

# If an extra older master baseline exists in slots 2-6, extract the same
# changed-file slice from it too, for print_uncovered_code.py's LBC
# cross-validation (intersecting them avoids false-positive LBC alerts from
# lines that only occasionally fire in background/async code). Nothing
# currently downloads these files, so this loop is presently a no-op.
for slot in 2 3 4 5 6; do
  src="base_llvm_coverage_${slot}.info"
  if [ -f "$src" ] && [ -s "$src" ]; then
    lcov --extract "$src" "${patterns[@]}" \
      --ignore-errors inconsistent,corrupt,empty,unsupported,unused \
      --quiet \
      -o "baseline_${slot}.changed.info"
    echo "Extracted changed-file slice from extra baseline #${slot}."
  fi
done

current_sf_count=$(grep -c '^SF:' current.changed.info 2>/dev/null || true)
baseline_sf_count=$(grep -c '^SF:' baseline.changed.info 2>/dev/null || true)

if [ "$current_sf_count" -eq 0 ] && [ "$baseline_sf_count" -eq 0 ]; then
  echo "No coverage data found for changed files (files may be new or not instrumented), skipping differential coverage report"
  echo "no_coverage_data" > "$OUTCOME_MARKER"
  exit 0
fi

if [ "$current_sf_count" -eq 0 ]; then
  # print_uncovered_code.py reads only current.changed.info, so it has no data
  # to report in this state either.
  echo "Current coverage is empty for changed files (tests may have been removed or disabled), skipping differential coverage report"
  echo "current_coverage_empty" > "$OUTCOME_MARKER"
  exit 0
fi

echo "Workspace path: $WORKSPACE_PATH"

HEADER_TITLE="differential coverage report"
if [ -n "${PR_NUMBER}" ]; then
    PR_URL="https://github.com/ClickHouse/ClickHouse/pull/${PR_NUMBER}"
    HEADER_TITLE="<a href=\"${PR_URL}\">${PR_URL}</a>"
fi

genhtml \
  --header-title "${HEADER_TITLE}" \
  --title "branch=${BRANCH}, current_commit=${CURRENT_COMMIT}" \
  --baseline-title "base_branch=${BASE_BRANCH}, baseline_commit=${FIRST_BASE_COMMIT}" \
  --baseline-file baseline.changed.info \
  --diff-file changes.diff \
  --output-directory llvm_coverage_diff_html_report \
  --no-function-coverage \
  --css-file $WORKSPACE_PATH/ci/jobs/scripts/css.css \
  --prefix $WORKSPACE_PATH \
  --substitute "s|/home/ubuntu/actions-runner/_work/ClickHouse/ClickHouse|$WORKSPACE_PATH|g" \
  --ignore-errors inconsistent \
  --ignore-errors corrupt \
  --ignore-errors path \
  --ignore-errors source \
  --ignore-errors range \
  --ignore-errors empty \
  --ignore-errors unused \
  --simplified-colors \
  --filter missing \
  --flat \
  current.changed.info

echo "report_generated" > "$OUTCOME_MARKER"
