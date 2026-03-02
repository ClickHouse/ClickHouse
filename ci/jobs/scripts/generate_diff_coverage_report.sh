#!/bin/bash
cd ci/tmp

if [[ ! -f "llvm_coverage.info" ]]; then
  echo "llvm_coverage.info not found"
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
echo "Warning: Could not find coverage file after checking ${#COMMITS[@]} commits"
echo "Skipping differential coverage analysis"
exit 0
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
  echo "Warning: no changed files reported by GitHub compare API"
  echo "Skipping differential coverage analysis"
  exit 0
fi

patterns=()
while IFS= read -r f; do
  [ -n "$f" ] && patterns+=("*$f")
done < <(echo "$changed_files")

lcov --extract llvm_coverage.info  "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt \
  --quiet \
  -o current.changed.info 2>/dev/null

lcov --extract base_llvm_coverage.info "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt \
  --quiet \
  -o baseline.changed.info 2>/dev/null

echo Workspace path: $WORKSPACE_PATH

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
  current.changed.info \
  2>/dev/null

