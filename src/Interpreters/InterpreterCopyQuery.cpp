#include <memory>
#include <Interpreters/InterpreterCopyQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTCopyQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
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
    auto ast_copy = std::static_pointer_cast<ASTCopyQuery>(query_ptr);
    if (ast_copy->type == ASTCopyQuery::QueryType::COPY_TO)
    {
        auto ast_table_func = ast_copy->file;
        if (!ast_table_func)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid file argument");

        if (!ast_copy->data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid select");

        auto context_lock = context.lock();
        auto table_function = TableFunctionFactory::instance().get(ast_table_func, context_lock);
        if (!table_function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such table function in copy command");

        auto table_storage = table_function->execute(ast_table_func, context_lock, "copy_files");
        auto storage_metadata = table_storage->getInMemoryMetadataPtr();
        auto sink = table_storage->write(std::make_shared<ASTInsertQuery>(), storage_metadata, context_lock, false);

        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);
        InterpreterSelectWithUnionQuery interpreter_select(ast_copy->data, context_lock, select_query_options);
        auto pipeline = interpreter_select.buildQueryPipeline();

        Chain out;
        out.addInterpreterContext(context_lock);
        out.addSource(std::move(sink));
        pipeline.addChain(std::move(out));

        BlockIO res;
        res.pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline));
        return res;
    }
    else
    {
        auto context_lock = context.lock();
        InterpreterInsertQuery interpreter_insert(
            ast_copy->data,
            context_lock,
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
