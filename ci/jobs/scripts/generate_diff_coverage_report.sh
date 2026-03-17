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

if [[ ! -f "llvm_coverage.info" ]]; then
  echo "ERROR: llvm_coverage.info not found"
  exit 1
fi

# Try to find .info file from S3, checking up to 30 ancestor commits
IFS=',' read -ra COMMITS <<< "${PREV_30_COMMITS}"

FOUND=0
for TEST_COMMIT in "${COMMITS[@]}"; do
COVERAGE_URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${TEST_COMMIT}/llvm_coverage/llvm_coverage.info"
echo "Checking coverage file for commit ${TEST_COMMIT}..."
if wget --spider "${COVERAGE_URL}" 2>&1 | grep -q '200 OK'; then
echo "Found coverage file at ${COVERAGE_URL}"
wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
FOUND=1
break
fi
done

if [ $FOUND -eq 0 ]; then
  echo "ERROR: Could not find baseline coverage file after checking ${#COMMITS[@]} commits"
  exit 1
fi

export CURRENT_COMMIT
export BASE_COMMIT
export PR_NUMBER
export REPO_NAME

gh api \
  -H "Accept: application/vnd.github.v3.diff" \
  repos/ClickHouse/ClickHouse/compare/${BASE_COMMIT}...${CURRENT_COMMIT} \
  > changes.diff
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

echo "Workspace path: $WORKSPACE_PATH"

HEADER_TITLE="differential coverage report"
if [ -n "${PR_NUMBER}" ]; then
    PR_URL="https://github.com/ClickHouse/ClickHouse/pull/${PR_NUMBER}"
    HEADER_TITLE="<a href=\"${PR_URL}\">${PR_URL}</a>"
fi

genhtml \
  --header-title "${HEADER_TITLE}" \
  --title "branch=${BRANCH}, current_commit=${CURRENT_COMMIT}" \
  --baseline-title "base_branch=${BASE_BRANCH}, baseline_commit=${BASE_COMMIT}" \
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

