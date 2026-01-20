When working with a branch, do not use rebase or amend - add new commits instead.

When writing text such as documentation, comments, or commit messages, wrap literal names from ClickHouse SQL language, classes and functions, or literal excerpts from log messages inside inline code blocks, such as: `MergeTree`.

When writing text such as documentation, comments, or commit messages, write names of functions and methods as `f` instead of `f()` - we prefer it for mathematical purity when it refers a function itself rather than its application.

When mentioning logical errors, say "exception" instead of "crash", because they don't crash the server in the release build.

Links to ClickHouse CI, such as `https://s3.amazonaws.com/clickhouse-test-reports/json.html?...` should be interpreted with a headless browser, e.g., Playwright, because they contain JavaScript. Use the tool at `.claude/tools/fetch_ci_report.js`:

```bash
# Install playwright if needed (one-time setup)
cd /tmp && npm install playwright && npx playwright install chromium

# Fetch and analyze CI report
node /path/to/ClickHouse/.claude/tools/fetch_ci_report.js "<ci-url>" [options]

# Options:
#   --test <name>    Filter tests by name
#   --failed         Show only failed tests
#   --all            Show all test results
#   --links          Show artifact links (logs.tar.gz, etc.)
#   --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz

# Examples:
node .claude/tools/fetch_ci_report.js "https://s3.amazonaws.com/..." --failed --links
node .claude/tools/fetch_ci_report.js "https://s3.amazonaws.com/..." --test peak_memory --download-logs
```

After downloading logs, extract specific test logs:
```bash
tar -xzf /tmp/ci_logs.tar.gz ci/tmp/pytest_parallel.jsonl
grep "test_name" ci/tmp/pytest_parallel.jsonl | python3 -c "import sys,json; [print(json.loads(l).get('longrepr','')) for l in sys.stdin if 'failed' in l]"
```

You can build multiple versions of ClickHouse inside `build_*` directories, such as `build`, `build_debug`, `build_asan`, etc.

You can run integration tests as in `tests/integration/README.md` using: `python -m ci.praktika run "integration" --test <selectors>` invoked from the repository root.
