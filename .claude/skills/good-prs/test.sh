#!/usr/bin/env bash
#
# Focused tests for report.sh. No network access: `gh` is replaced by a stub on
# PATH that serves canned fixtures, so the tests exercise report.sh's own logic
# (bucket -> label classification, the "only CH Inc sync is non-green" criterion,
# the public-repo conflict marker, empty-input handling, --repo pinning, and
# fail-loud-on-real-error behaviour).
#
# Usage: bash test.sh
#
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPORT="$HERE/report.sh"

# Keep the retry wrapper's timing out of the tests: same code path, no sleeping.
export GH_RETRIES=3 GH_RETRY_DELAY=0
# Same for the UNKNOWN-mergeability re-query: keep the attempts, drop the wait.
export GH_MERGEABLE_TRIES=3 GH_MERGEABLE_DELAY=0

RC=0
ok()  { printf '  ok   - %s\n' "$*"; }
bad() { printf '  FAIL - %s\n' "$*"; RC=1; }

# Write the `gh` stub into <dir>/bin. It dispatches on the gh subcommand and reads
# canned fixtures from $GH_STUB_DIR, logging every invocation to $GH_STUB_LOG.
write_stub()
{
  local bin="$1/bin"
  mkdir -p "$bin"
  cat > "$bin/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
printf '%s\n' "$*" >> "$GH_STUB_LOG"
sub="${1:-} ${2:-}"
case "$sub" in
  "api user")
    cat "$GH_STUB_DIR/me" ;;
  "pr list")
    mode=""; who=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --author)   mode=author;   who="$2"; shift 2 ;;
        --assignee) mode=assignee; who="$2"; shift 2 ;;
        *) shift ;;
      esac
    done
    f="$GH_STUB_DIR/list/${mode}_${who}.tsv"
    [ -f "$f" ] && cat "$f"
    ;;
  "pr checks")
    n="$3"
    # <n>.flaky holds a count of leading attempts that fail transiently, so a test
    # can assert that gh_retry keeps going and eventually returns the real answer.
    if [ -f "$GH_STUB_DIR/checks/$n.flaky" ]; then
      cnt=$(cat "$GH_STUB_DIR/checks/$n.count" 2>/dev/null || echo 0)
      cnt=$(( cnt + 1 )); printf '%s\n' "$cnt" > "$GH_STUB_DIR/checks/$n.count"
      if [ "$cnt" -le "$(cat "$GH_STUB_DIR/checks/$n.flaky")" ]; then
        echo "HTTP 503: 503 Service Unavailable (https://api.github.com/graphql)" >&2; exit 1
      fi
    fi
    if [ -f "$GH_STUB_DIR/checks/$n.err" ]; then cat "$GH_STUB_DIR/checks/$n.err" >&2; exit 1; fi
    if [ -f "$GH_STUB_DIR/checks/$n.nochecks" ]; then echo "no checks reported on the 'br-$n' branch" >&2; exit 1; fi
    cat "$GH_STUB_DIR/checks/$n.json"
    ;;
  "pr view")
    n="$3"
    if [ -f "$GH_STUB_DIR/view/$n.err" ]; then cat "$GH_STUB_DIR/view/$n.err" >&2; exit 1; fi
    # <n>.lazy holds a count of leading calls that answer with <n>.tsv.first,
    # mimicking GitHub's lazily computed mergeability (UNKNOWN until the test
    # merge it schedules has run).
    if [ -f "$GH_STUB_DIR/view/$n.lazy" ]; then
      cnt=$(cat "$GH_STUB_DIR/view/$n.vcount" 2>/dev/null || echo 0)
      cnt=$(( cnt + 1 )); printf '%s\n' "$cnt" > "$GH_STUB_DIR/view/$n.vcount"
      if [ "$cnt" -le "$(cat "$GH_STUB_DIR/view/$n.lazy")" ]; then
        cat "$GH_STUB_DIR/view/$n.tsv.first"; exit 0
      fi
    fi
    cat "$GH_STUB_DIR/view/$n.tsv"
    ;;
  *)
    echo "gh stub: unhandled args: $*" >&2; exit 99 ;;
esac
exit 0   # success branches return 0 even when a list fixture is absent (empty list)
STUB
  chmod +x "$bin/gh"
}

# run_report <stub-dir> -> sets globals: OUT, ERRF, EXIT
run_report()
{
  local d="$1"
  OUT="$d/out"; ERRF="$d/err"
  PATH="$d/bin:$PATH" GH_STUB_DIR="$d/fix" GH_STUB_LOG="$d/calls.log" \
    bash "$REPORT" > "$OUT" 2> "$ERRF"
  EXIT=$?
}

row_has() { # <out> <pr-num> <label> <desc>
  if grep -F "#$2" "$1" | grep -qF "$3"; then ok "$4"; else bad "$4"; fi
}
not_present() { # <out> <pr-num> <desc>
  if grep -qF "#$1" "$2"; then bad "$3"; else ok "$3"; fi
}

# Every PR-scoped gh call must be pinned to ClickHouse/ClickHouse.
assert_repo_pinned() { # <calls.log> <desc>
  if grep -E '^pr (list|checks|view)' "$1" | grep -vq -- '--repo ClickHouse/ClickHouse'; then
    bad "$2"
  else
    ok "$2"
  fi
}

############################ Test A: classification ############################
echo "Test A: bucket -> label classification and the only-sync criterion"
A=$(mktemp -d); write_stub "$A"; mkdir -p "$A/fix/list" "$A/fix/checks" "$A/fix/view"
printf 'testuser\n' > "$A/fix/me"
{
  printf '101\ttestuser\tFully green PR\n'
  printf '102\ttestuser\tSync failed PR\n'
  printf '103\ttestuser\tSync in progress PR\n'
  printf '104\ttestuser\tBackport, no sync check\n'
  printf '105\ttestuser\tNon-sync check failing\n'
  printf '106\ttestuser\tMerged between list and view\n'
  printf '107\ttestuser\tSync QUEUED\n'
  printf '108\ttestuser\tSync CANCELLED\n'
  printf '109\ttestuser\tSync SKIPPED\n'
  printf '110\ttestuser\tGreen but conflicting\n'
  printf '111\ttestuser\tConflict known only on the second ask\n'
  printf '112\ttestuser\tMergeability never computed\n'
} > "$A/fix/list/author_testuser.tsv"
printf '201\tgroeneai\tGroeneai green PR\n' > "$A/fix/list/author_groeneai.tsv"

c() { printf '%s\n' "$2" > "$A/fix/checks/$1.json"; }
c 101 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"},{"name":"Tests","state":"SKIPPED","bucket":"skipping"},{"name":"PR","state":"SUCCESS","bucket":"pass"},{"name":"Mergeable Check","state":"SUCCESS","bucket":"pass"}]'
c 102 '[{"name":"CH Inc sync","state":"FAILURE","bucket":"fail"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 103 '[{"name":"CH Inc sync","state":"IN_PROGRESS","bucket":"pending"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 104 '[{"name":"Build","state":"SUCCESS","bucket":"pass"},{"name":"Docs","state":"SKIPPED","bucket":"skipping"}]'
c 105 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Tests","state":"FAILURE","bucket":"fail"}]'
c 106 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 107 '[{"name":"CH Inc sync","state":"QUEUED","bucket":"pending"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 108 '[{"name":"CH Inc sync","state":"CANCELLED","bucket":"cancel"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 109 '[{"name":"CH Inc sync","state":"SKIPPED","bucket":"skipping"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 110 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 111 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 112 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'
c 201 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'

# v <pr> <state> <ever-approved> [mergeable, default MERGEABLE]
v() { printf '%s\t%s\t%s\n' "$2" "$3" "${4:-MERGEABLE}" > "$A/fix/view/$1.tsv"; }
v 101 OPEN true
v 102 OPEN false
v 103 OPEN false
v 104 OPEN false
v 106 MERGED false UNKNOWN
v 107 OPEN false
v 108 OPEN false
v 109 OPEN false
v 110 OPEN false CONFLICTING
v 111 OPEN false CONFLICTING
printf 'OPEN\tfalse\tUNKNOWN\n' > "$A/fix/view/111.tsv.first"
printf '1\n' > "$A/fix/view/111.lazy"     # UNKNOWN once, then the real verdict
v 112 OPEN false UNKNOWN
v 201 OPEN true

run_report "$A"
[ "$EXIT" = 0 ] && ok "exit code 0" || { bad "exit code 0 (got $EXIT)"; cat "$ERRF"; }
row_has "$OUT" 101 GREEN  "#101 fully green -> GREEN"
row_has "$OUT" 102 FAILED "#102 sync FAILURE -> FAILED"
row_has "$OUT" 103 INPROG "#103 sync IN_PROGRESS -> INPROG"
row_has "$OUT" 104 NOSYNC "#104 no sync check -> NOSYNC"
not_present 105 "$OUT" "#105 non-sync failure -> excluded"
not_present 106 "$OUT" "#106 merged -> excluded"
row_has "$OUT" 107 INPROG "#107 sync QUEUED (bucket pending) -> INPROG"
row_has "$OUT" 108 FAILED "#108 sync CANCELLED (bucket cancel) -> FAILED"
row_has "$OUT" 109 NOSYNC "#109 sync SKIPPED (bucket skipping) -> NOSYNC"
row_has "$OUT" 201 GREEN  "#201 groeneai section rendered"
row_has "$OUT" 110 'GREEN+CONFLICT' "#110 conflicting in the public repo -> GREEN+CONFLICT"
row_has "$OUT" 111 'GREEN+CONFLICT' "#111 conflict found on the re-query -> GREEN+CONFLICT"
row_has "$OUT" 112 'GREEN+UNKNOWN'  "#112 mergeability never computed -> GREEN+UNKNOWN"
if [ "$(grep -c '^pr view 111 ' "$A/calls.log")" = 2 ]; then
  ok "UNKNOWN mergeability re-queried until GitHub computed it"
else
  bad "UNKNOWN mergeability re-queried until GitHub computed it (got $(grep -c '^pr view 111 ' "$A/calls.log") calls)"
fi
if [ "$(grep -c '^pr view 106 ' "$A/calls.log")" = 1 ]; then
  ok "a non-OPEN PR's UNKNOWN mergeability is not re-queried"
else
  bad "a non-OPEN PR's UNKNOWN mergeability is not re-queried (got $(grep -c '^pr view 106 ' "$A/calls.log") calls)"
fi
if [ "$(grep -c '^pr view 112 ' "$A/calls.log")" = "$GH_MERGEABLE_TRIES" ]; then
  ok "a permanently UNKNOWN mergeability stops after GH_MERGEABLE_TRIES"
else
  bad "a permanently UNKNOWN mergeability stops after GH_MERGEABLE_TRIES (got $(grep -c '^pr view 112 ' "$A/calls.log") calls)"
fi
# Within one sync bucket: clean rows first, then unknown, then conflicting.
order=$(grep -oE '#(101|110|111|112)' "$OUT" | tr -d '#' | tr '\n' ' ')
if [ "$order" = "101 112 110 111 " ]; then
  ok "conflicting rows sorted after the clean ones in the same bucket"
else
  bad "conflicting rows sorted after the clean ones in the same bucket (got: $order)"
fi
if grep -q '| sync / merge |' "$OUT"; then ok "first column header names the merge state"; else bad "first column header names the merge state"; fi
rm -rf "$A"

############################ Test B: empty input ###############################
echo "Test B: no PRs at all -> clean empty sections, exit 0"
B=$(mktemp -d); write_stub "$B"; mkdir -p "$B/fix/list" "$B/fix/checks" "$B/fix/view"
printf 'testuser\n' > "$B/fix/me"
run_report "$B"
[ "$EXIT" = 0 ] && ok "exit code 0 on empty input" || { bad "exit code 0 on empty input (got $EXIT)"; cat "$ERRF"; }
if grep -q '## 1. Your own PRs' "$OUT"; then ok "section headers still rendered"; else bad "section headers still rendered"; fi
if grep -q '\[#' "$OUT"; then bad "no PR rows on empty input"; else ok "no PR rows on empty input"; fi
rm -rf "$B"

####################### Test C: real gh failure on checks ######################
echo "Test C: transient gh pr checks failure -> abort with diagnostic"
C=$(mktemp -d); write_stub "$C"; mkdir -p "$C/fix/list" "$C/fix/checks" "$C/fix/view"
printf 'testuser\n' > "$C/fix/me"
printf '301\ttestuser\tWill error\n' > "$C/fix/list/author_testuser.tsv"
printf 'HTTP 403: API rate limit exceeded\n' > "$C/fix/checks/301.err"
run_report "$C"
[ "$EXIT" != 0 ] && ok "non-zero exit on gh checks failure" || bad "non-zero exit on gh checks failure (got $EXIT)"
if grep -qF "'gh pr checks 301' failed" "$ERRF"; then ok "diagnostic names the failing PR"; else bad "diagnostic names the failing PR"; fi
if grep -qF "rate limit" "$ERRF"; then ok "original gh diagnostic preserved"; else bad "original gh diagnostic preserved"; fi
rm -rf "$C"

####################### Test D: real gh failure on view ########################
echo "Test D: transient gh pr view failure -> abort with diagnostic"
D=$(mktemp -d); write_stub "$D"; mkdir -p "$D/fix/list" "$D/fix/checks" "$D/fix/view"
printf 'testuser\n' > "$D/fix/me"
printf '401\ttestuser\tView errors\n' > "$D/fix/list/author_testuser.tsv"
printf '%s\n' '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"}]' > "$D/fix/checks/401.json"
printf 'HTTP 500: server error\n' > "$D/fix/view/401.err"
run_report "$D"
[ "$EXIT" != 0 ] && ok "non-zero exit on gh view failure" || bad "non-zero exit on gh view failure (got $EXIT)"
if grep -qF "'gh pr view 401' failed" "$ERRF"; then ok "diagnostic names the failing PR"; else bad "diagnostic names the failing PR"; fi
rm -rf "$D"

################## Test E: 'no checks reported' is not fatal ####################
echo "Test E: a 'no checks reported' PR is excluded, not fatal"
E=$(mktemp -d); write_stub "$E"; mkdir -p "$E/fix/list" "$E/fix/checks" "$E/fix/view"
printf 'testuser\n' > "$E/fix/me"
{ printf '501\ttestuser\tNo checks reported\n'; printf '502\ttestuser\tGreen PR\n'; } > "$E/fix/list/author_testuser.tsv"
: > "$E/fix/checks/501.nochecks"
printf '%s\n' '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"}]' > "$E/fix/checks/502.json"
v502() { printf 'OPEN\ttrue\tMERGEABLE\n' > "$E/fix/view/502.tsv"; }; v502
run_report "$E"
[ "$EXIT" = 0 ] && ok "exit code 0 (no-checks is legitimate)" || { bad "exit code 0 (got $EXIT)"; cat "$ERRF"; }
not_present 501 "$OUT" "#501 with no checks -> excluded"
row_has "$OUT" 502 GREEN "#502 green PR still rendered alongside excluded one"
rm -rf "$E"

################## Test F: a transient error is retried, not fatal ##############
echo "Test F: transient 503s are retried and the run still succeeds"
F=$(mktemp -d); write_stub "$F"; mkdir -p "$F/fix/list" "$F/fix/checks" "$F/fix/view"
printf 'testuser\n' > "$F/fix/me"
printf '601\ttestuser\tFlaky then green\n' > "$F/fix/list/author_testuser.tsv"
printf '2\n' > "$F/fix/checks/601.flaky"   # fail twice, succeed on the third call
printf '%s\n' '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"}]' > "$F/fix/checks/601.json"
printf 'OPEN\ttrue\tMERGEABLE\n' > "$F/fix/view/601.tsv"
run_report "$F"
[ "$EXIT" = 0 ] && ok "exit code 0 after retried 503s" || { bad "exit code 0 after retried 503s (got $EXIT)"; cat "$ERRF"; }
row_has "$OUT" 601 GREEN "#601 rendered after the transient failures"
if [ "$(grep -c '^pr checks 601 ' "$F/calls.log")" = 3 ]; then
  ok "gh pr checks retried exactly until it succeeded"
else
  bad "gh pr checks retried exactly until it succeeded (got $(grep -c '^pr checks 601 ' "$F/calls.log") calls)"
fi
rm -rf "$F"

############## Test G: a permanent error is not retried, still fatal ############
echo "Test G: a permanent error fails on the first attempt"
G=$(mktemp -d); write_stub "$G"; mkdir -p "$G/fix/list" "$G/fix/checks" "$G/fix/view"
printf 'testuser\n' > "$G/fix/me"
printf '701\ttestuser\tPermanently broken\n' > "$G/fix/list/author_testuser.tsv"
printf 'GraphQL: Could not resolve to a PullRequest with the number of 701.\n' > "$G/fix/checks/701.err"
run_report "$G"
[ "$EXIT" != 0 ] && ok "non-zero exit on permanent failure" || bad "non-zero exit on permanent failure (got $EXIT)"
if [ "$(grep -c '^pr checks 701 ' "$G/calls.log")" = 1 ]; then
  ok "permanent error not retried"
else
  bad "permanent error not retried (got $(grep -c '^pr checks 701 ' "$G/calls.log") calls)"
fi
rm -rf "$G"

echo
if [ "$RC" = 0 ]; then echo "ALL TESTS PASSED"; else echo "SOME TESTS FAILED"; fi
exit "$RC"
