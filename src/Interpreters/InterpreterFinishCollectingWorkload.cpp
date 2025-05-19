#include "InterpreterFinishCollectingWorkload.h"
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/IndexAdvisor/IndexAdvisor.h>
#include <Interpreters/IndexAdvisor/QueryInfo.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTStartCollectingWorkloadQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
extern const SettingsString collection_file_path;
}

BlockIO InterpreterFinishCollectingWorkload::execute()
{
    executeQuery("SET collect_workload = 0", context, QueryFlags{.internal = true});

    QueryInfo query_info(context->getSettingsRef()[Setting::collection_file_path], context);
    for (const auto & table : query_info.getTables()) {
        String columns_str;
        for(const auto& column: query_info.getColumns(table)){
            columns_str += column + ", ";
        }
        LOG_DEBUG(getLogger("InterpreterFinishCollectingWorkload"), "Table {} has columns {}", table, columns_str);
    }
    IndexAdvisor index_advisor(context, query_info);
    auto res = index_advisor.getBestPKColumns();
    MutableColumns columns;
    columns.emplace_back(ColumnString::create());
    columns.emplace_back(ColumnString::create());
    columns.emplace_back(ColumnString::create());

    for (const auto & [table_name, table_data] : res)
    {
        const auto & [pk_columns_list, estimation] = table_data;
        for (const auto & pk_columns : pk_columns_list)
        {
            columns[0]->insert(table_name);
            columns[1]->insert(pk_columns);
        }
    }

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(columns[0]), std::make_shared<DataTypeString>(), "table_name"});
    block.insert(ColumnWithTypeAndName{std::move(columns[1]), std::make_shared<DataTypeString>(), "primary_key_columns"});
    BlockIO block_io;
    block_io.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));
    return block_io;
}

void registerInterpreterFinishCollectingWorkload(InterpreterFactory & factory)
{
    auto create_fn = [](const InterpreterFactory::Arguments & args)
    { return std::make_unique<InterpreterFinishCollectingWorkload>(args.query, args.context); };
    factory.registerInterpreter("InterpreterFinishCollectingWorkload", create_fn);
}

}
