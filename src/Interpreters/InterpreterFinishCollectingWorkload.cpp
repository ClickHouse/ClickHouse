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
    extern const SettingsBool index_advisor_find_best_index;
    extern const SettingsBool index_advisor_find_best_pk;
}

BlockIO InterpreterFinishCollectingWorkload::execute()
{
    executeQuery("SET collect_workload = 0", context, QueryFlags{.internal = true});

    QueryInfo query_info(context->getSettingsRef()[Setting::collection_file_path], context);
    IndexAdvisor index_advisor(context, query_info);
    MutableColumns columns;
    columns.emplace_back(ColumnString::create());
    columns.emplace_back(ColumnString::create());
    columns.emplace_back(ColumnString::create());

    bool have_some_indexes = false;

    if (context->getSettingsRef()[Setting::index_advisor_find_best_pk]) {
        auto res = index_advisor.getBestPKColumns();
    
        for (const auto & [table_name, table_data] : res)
        {
            const auto & [pk_columns_list, estimation] = table_data;
            for (const auto & pk_columns : pk_columns_list)
            {
                columns[0]->insert(table_name);
                columns[1]->insert("primary_key");
                columns[2]->insert(pk_columns);
                have_some_indexes = true;
            }
        }
    }

    if (context->getSettingsRef()[Setting::index_advisor_find_best_index]) {
        auto res = index_advisor.getBestMinMaxIndexForTables();
        for (const auto & [table_name, minmax_index_columns] : res)
        {
            for (const auto & [minmax_index_column, index_type] : minmax_index_columns)
            {
                columns[0]->insert(table_name);
                columns[1]->insert(index_type);
                columns[2]->insert(minmax_index_column);
                have_some_indexes = true;
            }
        }
    }

    if (!have_some_indexes) {
        LOG_INFO(getLogger("InterpreterFinishCollectingWorkload"), "No indexes found");
        return {};
    }

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(columns[0]), std::make_shared<DataTypeString>(), "Table name"});
    block.insert(ColumnWithTypeAndName{std::move(columns[1]), std::make_shared<DataTypeString>(), "Index type"});
    block.insert(ColumnWithTypeAndName{std::move(columns[2]), std::make_shared<DataTypeString>(), "Columns for index"});

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
