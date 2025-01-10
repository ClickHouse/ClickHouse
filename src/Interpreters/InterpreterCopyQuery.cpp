#include <memory>
#include <ucontext.h>
#include <Interpreters/InterpreterCopyQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include "Common/Exception.h"
#include "Interpreters/InterpreterInsertQuery.h"
#include "Parsers/ASTInsertQuery.h"

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterCopyQuery::execute()
{
    auto* ast_copy = query_ptr->as<ASTCopyQuery>();
    if (ast_copy->type == ASTCopyQuery::QueryType::COPY_TO)
    {
        auto ast_table_func = ast_copy->file;
        if (!ast_table_func)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid file argument");

        if (!ast_copy->data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid select");

        auto table_function = TableFunctionFactory::instance().get(ast_table_func, getContext());
        if (!table_function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such table function in copy command");

        auto table_storage = table_function->execute(ast_table_func, getContext(), table_function->getName());
        auto storage_metadata = table_storage->getInMemoryMetadataPtr();
        auto sink = table_storage->write(std::make_shared<ASTInsertQuery>(), storage_metadata, getContext(), false);

        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);
        InterpreterSelectWithUnionQuery interpreter_select(ast_copy->data, getContext(), select_query_options);
        auto pipeline = interpreter_select.buildQueryPipeline();

        Chain out;
        out.addInterpreterContext(getContext());
        out.addSource(std::move(sink));
        pipeline.addChain(std::move(out));

        pipeline.setSinks(
            [&](const Block & cur_header, QueryPipelineBuilder::StreamType) -> ProcessorPtr
            { return std::make_shared<EmptySink>(cur_header); });

        BlockIO res;
        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline));
        return res;
    }
    else
    {
        InterpreterInsertQuery interpreter_insert(
            ast_copy->data,
            getContext(),
            /* allow_materialized */ false,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false);
        return interpreter_insert.execute();
    }
}

void registerInterpreterCopyQuery(InterpreterFactory & factory)
{
    auto create_fn
        = [](const InterpreterFactory::Arguments & args) { return std::make_unique<InterpreterCopyQuery>(args.query, args.context); };
    factory.registerInterpreter("InterpreterCopyQuery", create_fn);
}

}
