/// Byte-level libFuzzer target: the input is the query text executed by an in-process `clickhouse local`.
/// The harness lives in `LocalFuzzerRunner.cpp`; `json_ast_sql_execution_fuzzer` shares it.

#include <LocalFuzzerRunner.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    // If it's a merge coordinator don't start anything
    if (DB::LocalFuzzerRunner::isMergeRun(*argc, *argv))
        return 0;

    DB::LocalFuzzerRunner::initialize(argc, argv, "");
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    DB::LocalFuzzerRunner::runQuery(String(reinterpret_cast<const char *>(data), size));
    return 0;
}
