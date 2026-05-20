# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

ClickHouse uses CMake + Ninja. C++ standard is C++23. Requires CMake 3.25+.

```bash
# Configure (RelWithDebInfo is default if CMAKE_BUILD_TYPE is omitted)
cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -B build

# Build (do not pass -j, ninja auto-detects parallelism)
ninja -C build clickhouse

# The binary is at build/programs/clickhouse (a multi-call binary: clickhouse-server, clickhouse-client, clickhouse-local, etc.)
```

Build variants use separate directories:
```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -B build_debug
cmake -G Ninja -DSANITIZE=address -B build_asan
cmake -G Ninja -DSANITIZE=thread -B build_tsan
cmake -G Ninja -DSANITIZE=memory -B build_msan
cmake -G Ninja -DSANITIZE=undefined -B build_ubsan
cmake -G Ninja -DSANITIZE="address;undefined" -B build_asan_ubsan
```

Key CMake options: `-DENABLE_TESTS=ON` (default, for unit tests), `-DSPLIT_DEBUG_SYMBOLS=ON`, `-DWITH_COVERAGE=ON`.

## Testing

**Stateless functional tests** (primary test suite, in `tests/queries/0_stateless/`):
- Format: `NNNNN_test_name.sql` (or `.sh`/`.py`) + `NNNNN_test_name.reference` (expected output)
- Run against a running server:
  ```bash
  ./tests/clickhouse-test NNNNN_test_name --binary build/programs/clickhouse
  ```
- Create a new test (auto-assigns next number):
  ```bash
  tests/queries/0_stateless/add-test test_name        # creates .sql + .reference
  tests/queries/0_stateless/add-test test_name.sh      # creates .sh + .reference
  ```

**Integration tests** (Docker-based, in `tests/integration/`):
```bash
python -m ci.praktika run "integration" --test <selectors>
```

**Unit tests** (Google Test):
```bash
ninja -C build unit_tests_dbms
cd build && ./unit_tests_dbms
```

## Architecture

ClickHouse is a column-oriented OLAP DBMS. The server is a single C++ binary (`programs/server/`) using `programs/main.cpp` as the unified entry point for all tools (server, client, local, keeper, etc.).

**Query execution pipeline** (the path a SELECT query takes):

1. **Parsing** (`src/Parsers/`): SQL text → AST (`IAST` tree). The parser is a hand-written recursive descent parser.
2. **Analysis** (`src/Analyzer/`): AST → `QueryTree` with resolved identifiers, types, and functions.
3. **Planning** (`src/Planner/`): `QueryTree` → `QueryPlan` (a DAG of `QueryPlanStep` nodes representing logical operations).
4. **Pipeline construction** (`src/Processors/`): `QueryPlan` → `QueryPipeline` (a graph of `IProcessor` nodes connected by ports for physical execution).
5. **Execution** (`src/QueryPipeline/`): The pipeline executes processors in a pull-based model, passing `Block` objects (batches of columns) between processors.

**Key data abstractions** (`src/Core/`, `src/Columns/`, `src/DataTypes/`):
- `IColumn`: Abstract columnar storage. Concrete types: `ColumnVector`, `ColumnString`, `ColumnArray`, `ColumnNullable`, `ColumnLowCardinality`, etc.
- `IDataType`: Type metadata and serialization rules.
- `Block`: A batch of `ColumnWithTypeAndName` tuples — the unit of data flow between processors.
- `ActionsDAG` (`src/Interpreters/ActionsDAG.h`): DAG for expression evaluation within a pipeline step.

**Storage engines** (`src/Storages/`):
- `IStorage`: Base class for all table engines.
- `MergeTree` family (`src/Storages/MergeTree/`): The primary engine family. `MergeTreeData` is the core class managing parts, merges, and mutations. Variants (Replacing, Summing, Aggregating, Collapsing, etc.) differ in merge semantics.
- `StorageFactory`: Singleton registry — engines register themselves with feature flags (supports_settings, supports_ttl, supports_replication, etc.).

**Functions and aggregates** (`src/Functions/`, `src/AggregateFunctions/`):
- Factory pattern: `FunctionFactory` / `AggregateFunctionFactory` are singleton registries.
- Registration: each function calls `registerFunction<T>` in a `registerFunctions*.cpp` file, typically using `REGISTER_FUNCTION(Name)` macros.

**Key source directories**:
- `src/Interpreters/`: Query orchestration, `executeQuery` is the main entry point.
- `src/Processors/`: Processor-based execution (Sources, Sinks, Transforms, QueryPlan steps).
- `src/Access/`: Authentication, authorization, RBAC.
- `src/Databases/`: Database engine implementations.
- `src/Formats/`: Input/output format implementations (JSON, Parquet, CSV, etc.).
- `src/Coordination/`: ClickHouse Keeper (ZooKeeper-compatible coordination).
- `src/Disks/`: Storage abstraction layer (local, S3, etc.).

## Conventions

When working with a branch, do not use rebase or amend - add new commits instead.

Do not commit to the master branch. Create a new branch for every task.

When writing text such as documentation, comments, or commit messages, wrap literal names from ClickHouse SQL language, classes and functions, or literal excerpts from log messages inside inline code blocks, such as: `MergeTree`.

When writing text such as documentation, comments, or commit messages, write names of functions and methods as `f` instead of `f()` - we prefer it for mathematical purity when it refers a function itself rather than its application.

When mentioning logical errors, say "exception" instead of "crash", because they don't crash the server in the release build.

## CI Tools

Links to ClickHouse CI should be analyzed using the tool at `.claude/tools/fetch_ci_report.js`, which directly fetches the underlying JSON data without requiring a browser. It accepts GitHub PR URLs (fetches all CI reports) or direct S3/CI HTML URLs.

```bash
# Fetch all CI reports for a PR
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345"

# Show only failed tests with CIDB links
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345" --failed --cidb

# Fetch only a specific report from a PR (by index)
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345" --report 2

# Filter by test name, show artifact links
node .claude/tools/fetch_ci_report.js "<url>" --test peak_memory --links

# Download logs and show failed tests
node .claude/tools/fetch_ci_report.js "<url>" --failed --download-logs

# Options:
#   --test <name>               Filter tests by name
#   --failed                    Show only failed tests
#   --all                       Show all test results
#   --links                     Show artifact links (logs.tar.gz, etc.)
#   --cidb                      Show CIDB links for failed tests
#   --report <number>           For PR URLs: fetch only one specific report
#   --download-logs [path]      Download logs to path (default: /tmp/ci_logs.tar.{gz,zst})
#   --credentials <user,password>  HTTP Basic Auth for private repositories
```

After downloading logs, extract specific test logs:
```bash
tar -xzf /tmp/ci_logs.tar.gz ci/tmp/pytest_parallel.jsonl
grep "test_name" ci/tmp/pytest_parallel.jsonl | python3 -c "import sys,json; [print(json.loads(l).get('longrepr','')) for l in sys.stdin if 'failed' in l]"
```

To analyze CI performance comparison results (slower/faster queries, unstable queries), use the tool at `.claude/tools/fetch_perf_report.py`. It fetches the machine-readable `all-query-metrics.tsv` from S3 for each performance shard, filters to `client_time`, and classifies queries as changed or unstable using the same thresholds as `compare.sh`.

```bash
# Show performance changes for a PR (default: changed + unstable queries only)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345"

# Filter by architecture
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --arch amd

# Show only per-shard summary (no individual queries)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --summary

# Filter by test name
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --test group_by

# Show all queries (not just changes)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --all --sort times

# JSON output for structured analysis
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --json

# TSV output for piping
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --tsv

# Also accepts CI HTML URLs
python3 .claude/tools/fetch_perf_report.py "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=12345&sha=abc123"
```

Key options: `--arch <amd|arm|all>` to filter architecture, `--metric <name>` to change metric (default `client_time`), `--shard <n>` for a specific shard, `--test <name>` / `--query <text>` for substring filtering, `--sort <diff|times|threshold|test>` for ordering, `--summary` for shard-level overview only, `--json` / `--tsv` for machine-readable output.

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

When asked to analyze assembly, inspect generated code, find register spills, check branch density, compare codegen between builds, or investigate optimization opportunities in compiled functions, use the tool at `.claude/tools/analyze-assembly.py`. It disassembles functions from a compiled binary, builds a CFG, computes metrics (spill/branch/call density), and reports findings. Use it instead of manually running `llvm-objdump` or `llvm-nm`.

```bash
# Basic analysis of a function
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>"

# Search for overloaded/templated functions by regex
python3 .claude/tools/analyze-assembly.py <binary> "insertRangeFrom" --search

# Pick a specific overload from ambiguous results
python3 .claude/tools/analyze-assembly.py <binary> "insertRangeFrom" --search --select 3

# JSON output for structured analysis
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --format json

# Source-interleaved disassembly (needs debug info)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --source

# Microarchitectural analysis of loop bodies (--mcpu is required)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --mca --mcpu=znver3

# Profile-weighted analysis (re-ranks findings by runtime impact)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --perf-map tmp/perf.map.jsonl

# Compare codegen between two builds
python3 .claude/tools/analyze-assembly.py --before <old_binary> --after <new_binary> "<function_name>"

# Analyze function at a specific address (useful for heavily-templated symbols)
python3 .claude/tools/analyze-assembly.py <binary> 0x0dc7c780

# Verbose mode to see tool commands
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" -v
```

Key options: `--search` for regex matching, `--fuzzy` for substring matching, `--select N` to pick from ambiguous results, `--all` to analyze all matches, `--context N` to show surrounding symbols, `--max-instructions N` to control output size, `--mca --mcpu=<model>` for llvm-mca throughput analysis, `--perf-map <file>` for runtime-weighted scoring, `--before`/`--after` for diff mode. Hex addresses (e.g. `0x0dc7c780`) are resolved to the enclosing symbol automatically — useful when symbol names are too long for regex matching. The tool caches symbol tables by build-id for fast repeated queries.

You can build multiple versions of ClickHouse inside `build_*` directories, such as `build`, `build_debug`, `build_asan`, etc.

You can run integration tests as in `tests/integration/README.md` using: `python -m ci.praktika run "integration" --test <selectors>` invoked from the repository root.

When writing tests, do not add "no-*" tags (like "no-parallel") unless strictly necessarily.

When writing tests in tests/queries, prefer adding a new test instead of extending existing ones.

When adding a new test, consult `./tests/queries/0_stateless/add-test` to determine the correct name prefix for the new test.

When writing C++ code, always use Allman-style braces (opening brace on a new line). This is enforced by the style check in CI.

Never use sleep in C++ code to fix race conditions - this is stupid and not acceptable!

When writing messages, say ASan, not ASAN, and similar (because there are two words: Address Sanitizer).

When checking the CI status, pay attention to the comment from robot with the links first. Look at the Praktika reports first. The logs of GitHub actions usually contain less info.

Do not use `-j` argument with ninja; do not use `nproc` - let it decide automatically.

When building ClickHouse (running ninja), always redirect output to the build log file in the build directory. Always use a subagent to analyze the log and return only a concise summary.

When running tests, always redirect output to a log file in the build directory (e.g. `<build_directory>/test_<test_name>.log`). Use unique file names per test so multiple tests can run in parallel. Always use a subagent to analyze each log and return only a concise summary.

If I provided a URL with the CI report, logs, or examples, include it in the commit message.

When creating or updating a pull request, use `.github/PULL_REQUEST_TEMPLATE.md` as the PR body template. The body should contain: a short description of the change and motivation, then the Changelog category (leave one from the list), then the Changelog entry, then the Documentation entry checkbox. Do not invent a custom "## Summary" or "## Test plan" structure — follow the template exactly. The "Bug Fix" category should be used only for real bug fixes, while for fixing CI reports you can use the "CI Fix or improvement" category. Include the URL to CI report I provided if any. If the PR is about a CI failure, search for the corresponding open issues and provide a link in the PR description.

ARM machines in CI are not slow. They are similar to x86 in performance.

Use `tmp` subdirectory in the current directory for temporary files (logs, downloads, scripts, etc.), do not use `/tmp`. Create the directory if needed.

