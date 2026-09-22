#pragma once

#include <base/types.h>
#include <Interpreters/Context_fwd.h>

#include <functional>

/// In-process `clickhouse local` harness shared by the libFuzzer targets that execute SQL
/// (`clickhouse_fuzzer`, `json_ast_sql_execution_fuzzer`).
///
/// `clickhouse local` runs on a dedicated runner thread (`ClientBase::runLibFuzzer`, defined in
/// `LocalFuzzerRunner.cpp`, replaces its normal query loop when `USE_FUZZING_MODE` is set). The
/// fuzzer's main thread hands one query text at a time to the runner and waits for it to finish,
/// so libFuzzer's per-input timeout, RSS limit and crash handling apply to the query execution.
namespace DB::LocalFuzzerRunner
{

/// True for libFuzzer's `-merge=1` coordinator run, which must not start `clickhouse local`.
bool isMergeRun(int argc, const char * const * argv);

/// Starts `clickhouse local` on the runner thread and blocks until it accepts queries. Arguments
/// after `-ignore_remaining_args=1` are passed to `clickhouse local` (e.g. `--max_execution_time=10`);
/// `-timeout=N` is picked up to tell libFuzzer's real timeout from its periodic alarms.
/// `setup_queries` (may be empty) is executed by the runner before the first input; a failure there
/// aborts the process, because every later input would run against a broken fixture.
/// Registers an `atexit` handler that ends the process with `_exit` (see the definition for why
/// `clickhouse local` is not shut down): a target registers its own `atexit` printers *after* this call.
void initialize(const int * argc, char *** argv, const String & setup_queries);

/// Executes `query` on the runner thread and returns when it has finished. Query errors are
/// swallowed like in interactive mode (printed when `CLICKHOUSE_FUZZER_PRINT_QUERY_ERRORS=1` is set);
/// a non-`DB::Exception` exception aborts the process.
void runQuery(const String & query);

/// Runs `task` on the runner thread with the session context of the in-process `clickhouse local`
/// (the same one `runQuery` uses, so it sees the fixture and the session settings) and returns when it
/// has finished. Exceptions escaping `task` abort the process; the task must handle query errors itself.
void runOnRunnerThread(std::function<void(ContextMutablePtr)> task);

}
