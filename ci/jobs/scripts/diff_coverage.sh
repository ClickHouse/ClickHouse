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

CURRENT_COMMIT=$(git rev-parse HEAD)
BASE_COMMIT=$(git merge-base HEAD master)

# Try to find .info file from S3, checking up to 10 ancestor commits
FOUND=0
ATTEMPT=0
MAX_ATTEMPTS=10

IFS=',' read -ra COMMITS <<< "${PREV_10_COMMITS}"

FOUND=0
for TEST_COMMIT in "${COMMITS[@]}"; do
    COVERAGE_URL="https://clickhouse-builds.s3.amazonaws.com/REFs/master/${TEST_COMMIT}/llvm_coverage_merge/llvm_coverage.info"
    echo "Checking coverage file for commit ${TEST_COMMIT}..."
    if wget --spider "${COVERAGE_URL}" 2>&1 | grep -q '200 OK'; then
        echo "Found coverage file at ${COVERAGE_URL}"
        wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
        BASE_COMMIT="${TEST_COMMIT}"
        FOUND=1
        break
    fi
done


if [ $FOUND -eq 0 ]; then
    echo "Warning: Could not find coverage file after checking ${ATTEMPT} commits"
    echo "Skipping differential coverage analysis"
    exit 0
fi

# get diff between current commit and base commit
git diff ${BASE_COMMIT}..${CURRENT_COMMIT} --unified=3 > changes.diff

genhtml current_llvm_coverage.info \
  --baseline-file base_llvm_coverage.info \
  --diff-file changes.diff \
  --output-directory diff-html \
  --legend \
  --ignore-errors inconsistent \
  --ignore-errors corrupt


lcov --version
base_line_coverage=$(lcov --ignore-errors inconsistent,corrupt --summary base_llvm_coverage.info 2>/dev/null | awk '/^  lines\.*:/{gsub(/%/,"",$2); print $2}')
curr_line_coverage=$(lcov --ignore-errors inconsistent,corrupt --summary current_llvm_coverage.info 2>/dev/null | awk '/^  lines\.*:/{gsub(/%/,"",$2); print $2}')

python3 - <<PY
import sys

base = float("${base_line_coverage}")
current = float("${curr_line_coverage}")

delta = current - base

print(f"Baseline coverage : {base:.2f}%")
print(f"Current coverage  : {current:.2f}%")
print(f"Delta             : {delta:+.2f}%")

if current < base:
    print("Coverage degraded.")
    sys.exit(1)
else:
    print("Coverage did not degrade.")
    sys.exit(0)
PY