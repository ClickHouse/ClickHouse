#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <AggregateFunctions/registerAggregateFunctions.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    using namespace DB;

    static SharedContextHolder shared_context;
    static ContextMutablePtr context;

    auto initialize = [&]() mutable
    {
        shared_context = Context::createShared();
        context = Context::createGlobal(shared_context.get());
        context->makeGlobalContext();
        context->setApplicationType(Context::ApplicationType::LOCAL);

        registerAggregateFunctions();
        registerFormats();

        return true;
    };

    static bool initialized = initialize();
    (void) initialized;

    /// The input format is as follows:
    /// - format name on the first line,
    /// - table structure on the second line,
    /// - the data for the rest of the input.

    /** The corpus was generated as follows:

    i=0; find ../../../../tests/queries -name '*.sql' |
        xargs -I{} bash -c "tr '\n' ' ' <{}; echo" |
        rg -o -i 'CREATE TABLE\s+\w+\s+\(.+?\) ENGINE' |
        sed -r -e 's/CREATE TABLE\s+\w+\s+\((.+?)\) ENGINE/\1/i' | sort | uniq |
        while read line; do
            i=$((i+1));
            clickhouse-local --query "SELECT name FROM system.formats ORDER BY rand() LIMIT 1" >> $i;
            echo "$line" >> $i;
            echo $RANDOM >> $i;
            echo $i;
        done
    */

    /// Compile the code as follows:
    ///   mkdir build_asan_fuzz
    ///   cd build_asan_fuzz
    ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
    ///
    /// The fuzzer can be run as follows:
    ///   ../../../build_asan_fuzz/src/Formats/fuzzers/format_fuzzer corpus -jobs=64

    DB::ReadBufferFromMemory in(data, size);

    String format;
    readStringUntilNewlineInto(format, in);
    assertChar('\n', in);

    String structure;
    readStringUntilNewlineInto(structure, in);
    assertChar('\n', in);

    ColumnsDescription description = parseColumnsListFromString(structure, context);
    auto columns_info = description.getOrdinary();

    Block header;
    for (const auto & info : columns_info)
    {
        ColumnWithTypeAndName column;
        column.name = info.name;
        column.type = info.type;
        column.column = column.type->createColumn();
        header.insert(std::move(column));
    }

    InputFormatPtr input_format = context->getInputFormat(format, in, header, 13 /* small block size */);

    QueryPipeline pipeline(Pipe(std::move(input_format)));
    PullingPipelineExecutor executor(pipeline);
    Block res;
    while (executor.pull(res))
        ;

    return 0;
}
catch (...)
{
    return 1;
}
