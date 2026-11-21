#!/usr/bin/env bash
#
# Focused tests for report.sh. No network access: `gh` is replaced by a stub on
# PATH that serves canned fixtures, so the tests exercise report.sh's own logic
# (bucket -> label classification, the "only CH Inc sync is non-green" criterion,
# empty-input handling, --repo pinning, and fail-loud-on-real-error behaviour).
#
# Usage: bash test.sh
#
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPORT="$HERE/report.sh"

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
    if [ -f "$GH_STUB_DIR/checks/$n.err" ]; then cat "$GH_STUB_DIR/checks/$n.err" >&2; exit 1; fi
    if [ -f "$GH_STUB_DIR/checks/$n.nochecks" ]; then echo "no checks reported on the 'br-$n' branch" >&2; exit 1; fi
    cat "$GH_STUB_DIR/checks/$n.json"
    ;;
  "pr view")
    n="$3"
    if [ -f "$GH_STUB_DIR/view/$n.err" ]; then cat "$GH_STUB_DIR/view/$n.err" >&2; exit 1; fi
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
c 201 '[{"name":"CH Inc sync","state":"SUCCESS","bucket":"pass"},{"name":"Build","state":"SUCCESS","bucket":"pass"}]'

v() { printf '%s\t%s\n' "$2" "$3" > "$A/fix/view/$1.tsv"; }
v 101 OPEN true
v 102 OPEN false
v 103 OPEN false
v 104 OPEN false
v 106 MERGED false
v 107 OPEN false
v 108 OPEN false
v 109 OPEN false
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
assert_repo_pinned "$A/calls.log" "every PR-scoped gh call pins --repo ClickHouse/ClickHouse"
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
v502() { printf 'OPEN\ttrue\n' > "$E/fix/view/502.tsv"; }; v502
run_report "$E"
[ "$EXIT" = 0 ] && ok "exit code 0 (no-checks is legitimate)" || { bad "exit code 0 (got $EXIT)"; cat "$ERRF"; }
not_present 501 "$OUT" "#501 with no checks -> excluded"
row_has "$OUT" 502 GREEN "#502 green PR still rendered alongside excluded one"
rm -rf "$E"

echo
if [ "$RC" = 0 ]; then echo "ALL TESTS PASSED"; else echo "SOME TESTS FAILED"; fi
exit "$RC"
