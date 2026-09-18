# `json_ast_sql_parser_fuzzer`: structure-aware fuzzing of the SQL parser

## What it tests {#what-it-tests}

The target drives the ClickHouse SQL parser and the AST formatter with *structurally valid* input
instead of random bytes. It is modelled on the SQLite fuzzer of Chromium and the Tarantool SQL
fuzzer (see [structure-aware fuzzing](https://github.com/google/fuzzing/blob/master/docs/structure-aware-fuzzing.md#example-sqlite)),
but it reuses an intermediate representation that ClickHouse already has: the native JSON AST
serialization (`IAST::writeJSON`, `IAST::createFromJSON`, the `clickhouse_json` dialect and the
`parseQueryToJSON` / `formatQueryFromJSON` functions).

Every input goes through this pipeline:

```
binary protobuf (json_ast.proto, mutated by libprotobuf-mutator)
  -> JSON AST text                       (JSONASTProtoConverter.cpp)
  -> IAST::createFromJSON                (max_ast_depth / max_ast_elements enforced)
  -> IAST::checkDepth, IAST::checkSize
  -> IAST::formatWithSecretsOneLine      (SQL text, length limited)
  -> ParserQuery                         (the normal SQL parser, max_parser_depth / backtracks)
  -> format again and parse once more    (format -> parse -> format stability)
```

So it exercises, in one target:

- every `readJSON` implementation, i.e. the boundary that hardens the `clickhouse_json` dialect
  against ASTs the SQL parser could never produce;
- the AST formatters (`formatImpl`) on ASTs that are well-formed but unusual;
- the SQL parser on syntactically plausible, deeply structured statements;
- the idempotency of format -> parse -> format.

The target is parse-only and stateless: it does not create a `Context`, does not run the
analyzer or interpreters, and never touches a server or the filesystem (except the optional dump
file described below). The companion target `json_ast_sql_execution_fuzzer`
(programs/local/fuzzers) shares the generation stages and executes the SQL instead, see
[Execution fuzzer](#execution-fuzzer) below.

The stages live in `JSONASTFuzzerPipeline.h`/`.cpp` (part of the `json_ast_fuzzer_proto` library)
so that both targets count, dump and report in the same way.

### Expected outcomes and findings {#expected-outcomes-and-findings}

Rejected JSON, ASTs rejected by `createFromJSON` or by the limits, formatting code that refuses a
parser-impossible shape, and syntax errors in the generated SQL are normal. They are counted (see
the statistics below) and swallowed. A per-stage allow-list of `DB::Exception` codes defines what
is normal (`isExpectedException` in `json_ast_sql_parser_fuzzer.cpp`). Everything else is a
finding and makes the process abort after printing the JSON and the SQL, so libFuzzer stores the
input:

- a `LOGICAL_ERROR` or any other exception code that the stage is not allowed to throw;
- an exception that is not a `DB::Exception` (e.g. `std::out_of_range`, `Poco::Exception`);
- an exception while formatting an AST produced by the SQL parser itself;
- sanitizer reports, assertion failures, aborts and stack overflows, which are never intercepted.

`JSON_AST_FUZZER_STRICT=1` promotes three tolerated outcomes to findings: a validation exception
while formatting the AST built from JSON, formatted SQL that does not parse back, and an unstable
format -> parse -> format round trip. These are real defects (the JSON layer accepted a shape the
formatter or the parser cannot handle, or the formatter is not canonical), but there are known
cases, so the default mode does not stop on them.

## How the protobuf mutation works {#how-the-protobuf-mutation-works}

`json_ast.proto` mirrors the JSON AST representation generically rather than describing each of
the ~90 node types with its own message:

- `Node` has the `type` discriminator (`NodeType`, one enum value per name registered in
  `getASTFactory`), the generic `children` array and an open list of `Property`.
- `Property` is a `Key` (an enum with one value per JSON key read by any `readJSON`) and a `oneof`
  over the value kinds that appear in the JSON: boolean, signed and unsigned integer, double,
  free-form string, `KnownString`, nested `Node`, list of nodes, `FieldValue`, list of strings,
  plain `Object` (for `server_type`, `changes`, `targets`, `elements`, ...), list of objects, `null`.
- `FieldValue` is the `{"field_type": ..., "value": ...}` encoding of a `Field`, with nested
  elements for `Array`, `Tuple` and `Map`.
- `KnownString` enumerates every string constant that some `readJSON` accepts for an
  enumeration-like property (`union_mode`, join `kind`/`strictness`/`locality`, `Function.kind`,
  `command_type`, `query_type`, `field_type`, ...) plus a small vocabulary of identifiers, function,
  type, engine and format names.

The value kind of a property is chosen independently of its key on purpose: the same key has
different kinds in different node types (`value`, `cluster`, `settings`, `database`, `table`,
`first`, `elements`, `partitions`), and a mismatch is a cheap way to probe the type checks of
`JSONObjectReader`. Cross-field invariants (e.g. `list_of_modes.size() == list_of_selects.size() - 1`)
are not expressible in the schema either; the `readJSON` implementations are expected to reject
them, and the fuzzer verifies that they do so with a controlled exception.

libprotobuf-mutator (`DEFINE_BINARY_PROTO_FUZZER`) replaces libFuzzer's byte-level mutator with
message-level operations: add, delete or replace a field, switch a `oneof` to another kind, pick a
different enum value, mutate a string or a number, copy a subtree from another corpus element
(crossover). Because keys and enumeration values are enums, a mutation always yields a key or a
constant that the readers know, and a subtree copied from a `SELECT` seed into a `CREATE TABLE`
seed keeps its internal structure. The corpus is stored in the binary protobuf wire format.

The text of an enum value is the `(json_text)` option in the `.proto` for node types, field types
and known strings (several of them differ only in case or contain spaces), and the identifier
without the `K_` prefix for keys. When a `readJSON` starts reading a new key or accepting a new
constant, append it to the schema. Never renumber existing values: the binary corpus in
`tests/fuzz/json_ast_sql_parser_fuzzer.in` and the corpus accumulated by CI depend on the numbers.
`json_ast_seed_converter to-proto` fails on a JSON document that uses a key or a node type missing
from the schema, so regenerating the seed corpus is the check that the schema is complete.

## Building {#building}

The target is built with the other fuzzers. Use Clang, `-DENABLE_FUZZING=1` (which also enables
libprotobuf-mutator) and sanitizers:

```bash
cmake -S . -B build_fuzz -G Ninja -DCMAKE_BUILD_TYPE=None \
    -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 \
    -DENABLE_FUZZING=1 -DENABLE_PROTOBUF=1 -DSANITIZE=address,undefined \
    -DENABLE_THINLTO=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_BUZZHOUSE=0 \
    -DPARALLEL_LINK_JOBS=1
ninja -C build_fuzz json_ast_sql_parser_fuzzer json_ast_seed_converter
```

Both binaries land in `build_fuzz/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/`. The examples
below use

```bash
FUZZER=build_fuzz/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast_sql_parser_fuzzer
CONVERTER=build_fuzz/src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/json_ast_seed_converter
```

`ninja -C build_fuzz fuzzers` builds every fuzzer and moves them to `build_fuzz/programs/`, which is
what CI does (the converter is not a fuzzer and stays in place). `ENABLE_FUZZING`
compiles everything with `-fsanitize=fuzzer-no-link` and links executables whose name ends in
`_fuzzer` with libFuzzer, see the `FUZZER` block in the top-level `CMakeLists.txt`. For memory or
thread sanitizer builds replace `SANITIZE`.

## Running {#running}

```bash
mkdir -p tmp/json_ast_corpus
$FUZZER \
    -timeout=60 -rss_limit_mb=8192 -max_len=65536 \
    tmp/json_ast_corpus tests/fuzz/json_ast_sql_parser_fuzzer.in
```

The first directory receives new corpus elements, the second one is the committed seed corpus
(`tests/fuzz/json_ast_sql_parser_fuzzer.options` holds the same libFuzzer options for the CI
runner, `tests/fuzz/runner.py`). Useful additional libFuzzer flags: `-jobs=N -workers=N` for
parallel runs, `-max_total_time=SECONDS`, `-runs=0` to only replay the corpus.

The target has its own limits, passed after `-ignore_remaining_args=1` like for the other parser
fuzzers (defaults in parentheses):

```bash
$FUZZER tmp/json_ast_corpus -ignore_remaining_args=1 \
    -max_ast_depth=200 -max_ast_elements=10000 -max_parser_depth=150 \
    -max_parser_backtracks=1000000 -max_json_length=1048576 -max_sql_length=262144
```

(`max_parser_depth` defaults to 150 in sanitizer and debug builds and to 300 otherwise.) The
rendered JSON is additionally bounded to four nesting levels per AST level, and libFuzzer's
`-max_len` bounds the serialized protobuf.

At exit the target prints how far the inputs got:

```
json_ast_sql_parser_fuzzer stage statistics:
  inputs:                         ...
  protobuf -> JSON rejected:      ...   limits of the renderer
  JSON -> AST rejected:           ...   createFromJSON / AST limits
  AST created:                    ...   (% of inputs)
  AST formatting rejected:        ...
  SQL too long:                   ...
  SQL generated:                  ...   (% of inputs)
  SQL parse rejected:             ...
  SQL parsed:                     ...   (% of inputs)
  format/parse round trip stable: ...   (% of parsed)
  round trip unstable:            ...
  re-parse rejected:              ...
```

Set `JSON_AST_FUZZER_STATS=0` to suppress it.

## Reproducing a crash and showing the generated SQL {#reproducing-a-crash}

libFuzzer writes the failing protobuf to `crash-<sha1>` (or `timeout-...`, `oom-...`). Replay it
with the dump switched on to see the JSON document and the SQL that were derived from it:

```bash
JSON_AST_FUZZER_DUMP=1 $FUZZER crash-<sha1>
JSON_AST_FUZZER_DUMP=tmp/repro.txt $FUZZER crash-<sha1>
```

`JSON_AST_FUZZER_DUMP=1` (or `stderr`) prints to stderr, any other value is a file path that is
appended to. The dump has three sections: `JSON AST`, `generated SQL` and, when the SQL parsed,
`SQL after parse and format`. For a finding the same sections are printed right before the abort
even without the variable. The dump is written for every input, so use it only when replaying
single inputs or a small corpus.

To reproduce outside the fuzzer:

```bash
# the JSON document the fuzzer fed to IAST::createFromJSON
$CONVERTER to-json crash-<sha1> > tmp/crash.json

# the same conversion on a server or clickhouse-local
clickhouse local --query "SELECT formatQueryFromJSON(\$\$$(cat tmp/crash.json)\$\$)"
clickhouse local --dialect clickhouse_json --enable_json_ast_dialect 1 --query "$(cat tmp/crash.json)"
```

A crash in the SQL parsing or the formatting stage can also be reproduced with the plain SQL from
the `generated SQL` section, e.g. with `select_parser_fuzzer` or `clickhouse format`.

## Seed corpus {#seed-corpus}

`tests/fuzz/json_ast_sql_parser_fuzzer.in/` holds a small binary corpus generated from
`seed_queries.sql`: one statement per line, covering `SELECT` clauses, `WITH`/CTE, `UNION`/
`INTERSECT`/`EXCEPT`, all `JOIN` kinds and `ARRAY JOIN`, lambdas, arrays, tuples and maps, window
functions, subqueries, table functions, `CREATE TABLE` with the data types, codecs, indices,
projections, constraints, TTL and settings, `CREATE` of views, dictionaries, databases and
functions, `ALTER`, `INSERT ... SELECT`, `SET`, `SYSTEM`, `SHOW`, `EXPLAIN`, `BACKUP`/`RESTORE`
and the other statements that have a JSON serialization. Regenerate it after changing the list or
the schema:

```bash
src/Parsers/fuzzers/json_ast_sql_parser_fuzzer/generate_seed_corpus.py \
    --clickhouse build/programs/clickhouse \
    --converter $CONVERTER
```

The script runs `parseQueryToJSON` on every statement (any `clickhouse` binary works, the
statements are only parsed), converts the JSON to protobuf, converts it back and compares, so a
seed that the schema cannot represent fails the run. Statements whose AST nodes have no
`writeJSON` (e.g. `GRANT`, `SHOW CREATE`, `INSERT ... VALUES`) cannot be seeds.

## Execution fuzzer {#execution-fuzzer}

`json_ast_sql_execution_fuzzer` (programs/local/fuzzers/json_ast_sql_execution_fuzzer.cpp) runs the
same protobuf -> JSON -> AST -> SQL stages and then executes the SQL in an in-process
`clickhouse local`, using the runner thread harness of `clickhouse_fuzzer`
(programs/local/fuzzers/LocalFuzzerRunner.h). Before the first input the runner executes the fixture
`json_ast_sql_execution_fuzzer_schema.sql`, which is embedded into the binary:

- `t`: a `MergeTree` table with ~40 columns covering the scalar, nullable, low-cardinality, array,
  tuple, map, nested, `JSON`, `Dynamic`, `Variant`, decimal, IP, UUID and enum types, with skip
  indexes and a projection, 1000 rows;
- `t1`, `t2` (join keys and timestamps), `t3`, `src`, `dst`, `tbl`, `empsalary` (window function
  seeds), `lg` (`Log`), `nul` (`Null`), `jt` (`Join`), `st` (`Set`);
- the view `v`, the materialized view `mv` and the dictionary `d`.

The names are part of the `KnownString` vocabulary in `json_ast.proto` and of the seed queries, so
mutated statements reference existing tables and columns and reach the analyzer, the planner and the
processors instead of failing on name resolution. The fixture ends with `SET readonly = 2`, and only
`SELECT`, `EXPLAIN`, `SHOW` and `CHECK TABLE` statements are executed (the rest is counted as
skipped), so a mutated statement cannot destroy the fixture.

Query errors are expected outcomes. Findings are crashes, sanitizer reports, `LOGICAL_ERROR`
exceptions (fatal in sanitizer and debug builds) and hangs. `tests/fuzz/json_ast_sql_execution_fuzzer.options`
passes `--max_execution_time=2`, `--max_rows_to_read` and network timeouts to `clickhouse local`, like `clickhouse_fuzzer`.

```bash
ninja -C build_fuzz json_ast_sql_execution_fuzzer
EXEC_FUZZER=build_fuzz/programs/local/fuzzers/json_ast_sql_execution_fuzzer
mkdir -p tmp/json_ast_exec_corpus
$EXEC_FUZZER -detect_leaks=0 -timeout=60 -rss_limit_mb=8192 -max_len=65536 -jobs=2 -workers=2 -max_total_time=2400 \
    tmp/json_ast_exec_corpus tests/fuzz/json_ast_sql_parser_fuzzer.in \
    -ignore_remaining_args=1 --output-format=Null --max_execution_time=2 --max_rows_to_read=1000000 \
    --max_memory_usage=2000000000 --connect_timeout=1 --external_storage_connect_timeout_sec=1 \
    --http_connection_timeout=1 --http_send_timeout=1 --http_receive_timeout=1 --http_max_tries=1
```

A query costs a few hundred milliseconds in a sanitizer build, so expect a few executions per
second per worker. Prefer `-jobs=N -workers=N` over `-fork=N` for this target: every fork-mode job
starts a fresh process that loads the fixture and replays the whole corpus, which dominates once the
corpus has a few thousand inputs. Keep the corpus small with `-merge=1` (below), and keep the network
and read limits from `tests/fuzz/json_ast_sql_execution_fuzzer.options`, otherwise mutants that call
`mysql`, `remote` or `url`, or read from `numbers()` without a limit, wait for the timeouts.

It consumes the same seed corpus as the parser fuzzer (`tests/fuzz/build.sh` copies it under the
execution fuzzer's name). `JSON_AST_FUZZER_DUMP` and `JSON_AST_FUZZER_STATS` work as above;
`JSON_AST_FUZZER_SCHEMA=<path>` replaces the built-in fixture with the statements from a file, e.g.
to fuzz against a different schema. Reproduce a crash with the same `JSON_AST_FUZZER_DUMP=1 $EXEC_FUZZER crash-<sha1>`
recipe; the `generated SQL` section can then be run with `clickhouse local --queries-file` after the
fixture.

## Minimizing a corpus {#minimizing-a-corpus}

libFuzzer's merge mode keeps only inputs that add coverage. The output directory must exist and
should be empty:

```bash
mkdir -p tmp/json_ast_corpus_min
$FUZZER -merge=1 tmp/json_ast_corpus_min tmp/json_ast_corpus tests/fuzz/json_ast_sql_parser_fuzzer.in
```

To shrink a single crashing input:

```bash
$FUZZER -minimize_crash=1 -runs=100000 crash-<sha1>
```

The CI runner (`tests/fuzz/runner.py`, job `libFuzzer tests`) performs the merge with the corpus
stored in S3 before every fuzzing session and uploads the merged corpus afterwards.
