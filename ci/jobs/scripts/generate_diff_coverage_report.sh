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

BASE_S3_PREFIX="https://clickhouse-builds.s3.amazonaws.com/REFs/master"

# Two passes over the same ancestor list.
#
# Pass 1 prefers an ancestor the job will actually be able to compare against.
# Most master runs are not complete, so selecting the first ancestor that merely
# has an .info would make the gate abstain on the majority of runs rather than
# judge them.
#
# Its predicate is llvm_coverage_completeness.comparable itself, not a local
# approximation of it. A selection test weaker than the usability test lets a
# candidate win here that the job then rejects, which turns "walk on to the next
# usable baseline" into a whole-job abstention. Reusing the one function keeps the
# two from drifting apart again, and it is why the candidate .info is downloaded
# before the decision: the torn-pair check digests the very bytes that would be
# compared.
#
# Pass 2 is byte-for-byte the previous behaviour, and it is what keeps this
# backward compatible: no master commit published before this change carries
# completeness metadata, so until master republishes, pass 1 finds nothing, pass 2
# selects exactly as before, and the job reports SKIPPED with a reason instead of
# failing.
FOUND=0
FIRST_BASE_COMMIT=""
for PASS in prefer_complete any_info; do
  if [ "$PASS" = "prefer_complete" ] && [ ! -f llvm_coverage.meta.json ]; then
    echo "No local completeness metadata; skipping the complete-baseline pass"
    continue
  fi
  echo "Baseline selection pass: ${PASS}"
  for TEST_COMMIT in "${COMMITS[@]}"; do
    COVERAGE_URL="${BASE_S3_PREFIX}/${TEST_COMMIT}/llvm_coverage/llvm_coverage.info"
    META_URL="${BASE_S3_PREFIX}/${TEST_COMMIT}/llvm_coverage/llvm_coverage.meta.json"
    echo "Checking coverage file for commit ${TEST_COMMIT}..."
    if ! wget --spider "${COVERAGE_URL}" 2>&1 | grep -q '200 OK'; then
      continue
    fi
    rm -f base_llvm_coverage.meta.json
    # Fetch the sidecar when it exists; the job validates the pair either way.
    wget --quiet "${META_URL}" -O base_llvm_coverage.meta.json || rm -f base_llvm_coverage.meta.json
    if [ "$PASS" = "prefer_complete" ]; then
      if [ ! -f base_llvm_coverage.meta.json ]; then
        echo "  no completeness metadata for ${TEST_COMMIT}"
        continue
      fi
      wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
      if ! REASON=$(PYTHONPATH="${WORKSPACE_PATH}" python3 -c "
import sys
from ci.jobs.scripts import llvm_coverage_completeness as completeness

ok, reason = completeness.comparable(
    completeness.read_sidecar('llvm_coverage.meta.json'),
    completeness.read_sidecar('base_llvm_coverage.meta.json'),
    baseline_info_path='base_llvm_coverage.info',
)
if ok:
    sys.exit(0)
sys.stdout.write(reason)
sys.exit(1)
"); then
        rm -f base_llvm_coverage.meta.json base_llvm_coverage.info
        echo "  ${TEST_COMMIT} is not usable as a baseline: ${REASON}"
        continue
      fi
      echo "  ${TEST_COMMIT} is a usable baseline for this measurement"
    else
      wget --quiet "${COVERAGE_URL}" -O base_llvm_coverage.info
    fi
    echo "Found coverage file at ${COVERAGE_URL}"
    FIRST_BASE_COMMIT="${TEST_COMMIT}"
    FOUND=1
    break
  done
  if [ $FOUND -eq 1 ]; then
    break
  fi
done

if [ $FOUND -eq 0 ]; then
  echo "ERROR: Could not find baseline coverage file after checking ${#COMMITS[@]} commits"
  exit 1
fi

# Record which ancestor was actually selected. The job cannot re-derive it: its
# own base_commit_sha is the NEAREST ancestor, generally not this one, and
# re-walking S3 from Python could select a different commit than the .info that
# ended up on disk.
echo "${FIRST_BASE_COMMIT}" > selected_base_commit.txt

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
gh api \
  -H "Accept: application/vnd.github.v3.diff" \
  repos/ClickHouse/ClickHouse/compare/${FIRST_BASE_COMMIT}...${CURRENT_COMMIT} \
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
  exit 0
fi

if [ "$current_sf_count" -eq 0 ]; then
  echo "Current coverage is empty for changed files (tests may have been removed or disabled). Skipping genhtml — LBC analysis will run separately."
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

