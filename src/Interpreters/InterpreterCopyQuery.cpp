#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterCopyQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
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

#include <memory>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

BlockIO InterpreterCopyQuery::execute()
{
    auto ast_copy = query_ptr->as<ASTCopyQuery &>();
    if (ast_copy.type == ASTCopyQuery::QueryType::COPY_TO)
    {
        auto ast_table_func = ast_copy.file;
        if (!ast_table_func)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid file argument");

        if (!ast_copy.data)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Copy should contain valid select");

        auto current_context = getContext();
        auto table_function = TableFunctionFactory::instance().get(ast_table_func, current_context);
        if (!table_function)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No such table function in copy command");

        if (ast_copy.data && table_function->needStructureHint())
        {
            Block header_block;
            auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);

            if (current_context->getSettingsRef()[Setting::allow_experimental_analyzer])
            {
                header_block = InterpreterSelectQueryAnalyzer::getSampleBlock(ast_copy.data, current_context, select_query_options);
            }
            else
            {
                InterpreterSelectWithUnionQuery interpreter_select{ast_copy.data, current_context, select_query_options};
                auto tmp_pipeline = interpreter_select.buildQueryPipeline();
                header_block = tmp_pipeline.getHeader();
            }

            ColumnsDescription structure_hint{header_block.getNamesAndTypesList()};
            table_function->setStructureHint(structure_hint);
        }

        const auto & function_name = table_function->getName();
        auto table_storage = table_function->execute(ast_table_func, current_context, function_name);

        const Settings & settings = getContext()->getSettingsRef();
        auto table_lock = table_storage->lockForShare(getContext()->getInitialQueryId(), settings[Setting::lock_acquire_timeout]);

        auto storage_metadata = table_storage->getInMemoryMetadataPtr();
        auto sink = table_storage->write(std::make_shared<ASTInsertQuery>(), storage_metadata, getContext(), false);

        auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);
        InterpreterSelectWithUnionQuery interpreter_select(ast_copy.data, getContext(), select_query_options);
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
        res.pipeline.addStorageHolder(table_storage);
        return res;
    }
    else
    {
        InterpreterInsertQuery interpreter_insert(
            ast_copy.data,
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
