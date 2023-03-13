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

    DB::ReadBufferFromMemory in(data, size);

    String format;
    readStringUntilNewlineInto(format, in);
    assertChar('\n', in);

    String structure;
    readStringUntilNewlineInto(structure, in);
    assertChar('\n', in);

    ColumnsDescription description;
    parseColumnsListFromString(structure, context);
    auto columns_info = description.getOrdinary();

    Block header;
    for (auto & info : columns_info)
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
