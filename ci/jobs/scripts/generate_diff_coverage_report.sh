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


cd ci/tmp

mv llvm_coverage.info current_llvm_coverage.info

# Try to find .info file from S3, checking up to 30 ancestor commits
FOUND=0
ATTEMPT=0
MAX_ATTEMPTS=30

IFS=',' read -ra COMMITS <<< "${PREV_30_COMMITS}"

FOUND=0
for TEST_COMMIT in "${COMMITS[@]}"; do
COVERAGE_URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${TEST_COMMIT}/llvm_coverage_merge/llvm_coverage.info"
echo "Checking coverage file for commit ${TEST_COMMIT}..."
if wget --spider "${COVERAGE_URL}" 2>&1 | grep -q '200 OK'; then
echo "Found coverage file at ${COVERAGE_URL}"
wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
FOUND=1
break
fi
done


if [ $FOUND -eq 0 ]; then
echo "Warning: Could not find coverage file after checking ${MAX_ATTEMPTS} commits"
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

# Build --include args
include_args=""
for f in $changed_files; do
    include_args="$include_args --include */$f"
done

patterns=()
while IFS= read -r f; do
  [ -n "$f" ] && patterns+=("*$f")
done < <(echo "$changed_files")

lcov --extract current_llvm_coverage.info  "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt \
  --quiet \
  -o current.changed.info 2>/dev/null

lcov --extract base_llvm_coverage.info "${patterns[@]}" \
  --ignore-errors inconsistent,corrupt \
  --quiet \
  -o baseline.changed.info 2>/dev/null

echo Workspace path: $WORKSPACE_PATH

# differential: PR link, current commit, current branch, baseline branch, baseline commit, ссылка на diff

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
  --flat \
  $include_args \
  current.changed.info \
  2>/dev/null

