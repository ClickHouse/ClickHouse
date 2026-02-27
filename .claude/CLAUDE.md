When working with a branch, do not use rebase or amend - add new commits instead.

When writing text such as documentation, comments, or commit messages, wrap literal names from ClickHouse SQL language, classes and functions, or literal excerpts from log messages inside inline code blocks, such as: `MergeTree`.

When writing text such as documentation, comments, or commit messages, write names of functions and methods as `f` instead of `f()` - we prefer it for mathematical purity when it refers a function itself rather than its application.

When mentioning logical errors, say "exception" instead of "crash", because they don't crash the server in the release build.

Links to ClickHouse CI, such as `https://s3.amazonaws.com/clickhouse-test-reports/json.html?...` should be analyzed using the tool at `.claude/tools/fetch_ci_report.js`, which directly fetches the underlying JSON data without requiring a browser:

```bash
# Fetch and analyze CI report
node /path/to/ClickHouse/.claude/tools/fetch_ci_report.js "<ci-url>" [options]

# Options:
#   --test <name>    Filter tests by name
#   --failed         Show only failed tests
#   --all            Show all test results
#   --links          Show artifact links (logs.tar.gz, etc.)
#   --download-logs  Download logs.tar.gz to /tmp/ci_logs.tar.gz
#   --credentials <user,password>  HTTP Basic Auth for private repositories

# Examples:
node .claude/tools/fetch_ci_report.js "https://s3.amazonaws.com/..." --failed --links
node .claude/tools/fetch_ci_report.js "https://s3.amazonaws.com/..." --test peak_memory --download-logs
```

After downloading logs, extract specific test logs:
```bash
tar -xzf /tmp/ci_logs.tar.gz ci/tmp/pytest_parallel.jsonl
grep "test_name" ci/tmp/pytest_parallel.jsonl | python3 -c "import sys,json; [print(json.loads(l).get('longrepr','')) for l in sys.stdin if 'failed' in l]"
```

To compile and run C++ code snippets against the ClickHouse codebase without modifying any source files, use the tool at `.claude/tools/cppexpr.sh`. This is a wrapper around `utils/c++expr` that auto-detects build directories and handles working directory setup. When asked about the size, layout, or alignment of ClickHouse data structures, or asked to compare performance of code snippets, use this tool to get a definitive answer instead of guessing.

```bash
# Query the size of a ClickHouse data structure
.claude/tools/cppexpr.sh -i Core/Block.h 'OUT(sizeof(DB::Block))'

# Query multiple expressions at once
.claude/tools/cppexpr.sh -i Core/Field.h 'OUT(sizeof(DB::Field)) OUT(sizeof(DB::Array))'

# Use global code for helper functions or custom types
.claude/tools/cppexpr.sh -g 'struct Foo { int a; double b; };' 'OUT(sizeof(Foo)) OUT(alignof(Foo))'

# Benchmark a code snippet (100000 iterations, 5 tests)
.claude/tools/cppexpr.sh -i Common/Stopwatch.h -b 100000 'Stopwatch sw;'

# Standalone mode (no ClickHouse headers, just standard C++)
.claude/tools/cppexpr.sh --plain 'OUT(sizeof(std::string))'
```

Key options: `-i HEADER` to include headers, `-g 'CODE'` for global-scope code, `-b STEPS` for benchmarking, `-l LIB` to link extra libraries, `--plain` for standalone compilation without ClickHouse. The `OUT(expr)` macro prints `expr -> value`.

You can build multiple versions of ClickHouse inside `build_*` directories, such as `build`, `build_debug`, `build_asan`, etc.

You can run integration tests as in `tests/integration/README.md` using: `python -m ci.praktika run "integration" --test <selectors>` invoked from the repository root.

When writing tests, do not add "no-*" tags (like "no-parallel") unless strictly necessarily.

When writing tests in tests/queries, prefer adding a new test instead of extending existing ones.

When writing C++ code, always use Allman-style braces (opening brace on a new line). This is enforced by the style check in CI.

Never use sleep in C++ code to fix race conditions - this is stupid and not acceptable!

When writing messages, say ASan, not ASAN, and similar (because there are two words: Address Sanitizer).

When checking the CI status, pay attention to the comment from robot with the links first. Look at the Praktika reports first. The logs of GitHub actions usually contain less info.

Do not use `-j` argument with ninja - let it decide automatically.

If I provided a URL with the CI report, logs, or examples, include it in the commit message.

When creating a pull request, append Changelog category and Changelog entry according to this template: `.github/PULL_REQUEST_TEMPLATE.md`. The "Bug Fix" category should be used only for real bug fixes, while for fixing CI reports you can use the "CI Fix or improvement" category. Include the URL to CI report I provided if any. If the PR is about a CI failure, search for the corresponding open issues and provide a link in the PR description.

ARM machines in CI are not slow. They are similar to x86 in performance.

Always load and apply the following skills:

- .claude/skills/build
- .claude/skills/test
- .claude/skills/fix-sync
- .claude/skills/alloc-profile
- .claude/skills/bisect
- .claude/skills/create-worktree
