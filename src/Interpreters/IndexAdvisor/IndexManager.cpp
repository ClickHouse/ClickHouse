#include "IndexManager.h"
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Core/Defines.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <QueryPipeline/QueryPipeline.h>

#include <fmt/format.h>


namespace DB
{

namespace Setting
{
extern const SettingsUInt64 mutations_sync;
}


namespace
{

UInt64 getReadMarks(const String & query, ContextMutablePtr context)
{
    auto io_exp = executeQuery("EXPLAIN ESTIMATE " + query, context, QueryFlags{.internal = true}).second;
    io_exp.pipeline.setNumThreads(1);
    PullingPipelineExecutor exec(io_exp.pipeline);
    Block block;
    UInt64 sum_read = 0;
    while (exec.pull(block))
    {
        // Get sum of read marks from column 4, which is the "read_marks" column in the EXPLAIN ESTIMATE output.
        const auto & col = block.getByPosition(4);
        for (size_t j = 0; j < col.column->size(); ++j)
        {
            sum_read += col.column->getUInt(j);
        }
    }
    return sum_read;
}

void createViews(ContextMutablePtr context, const Strings & views)
{
    for (const auto & view : views)
    {
        executeQuery(view, context, QueryFlags{.internal = true});
    }
}

void dropViews(ContextMutablePtr context, const Strings & views)
{
    for (const auto & view : views)
    {
        executeQuery(view, context, QueryFlags{.internal = true});
    }
}
}

bool IndexManager::addIndex(const String & index_name, const String & index_columns, const String & index_type)
{
    // Make sure the index will be updated synchronously
    auto prev_mutation_setting = context->getSettingsRef()[Setting::mutations_sync].value;
    context->setSetting("mutations_sync", 2);

    try
    {
        String add_index_query
            = "ALTER TABLE " + table + " ADD INDEX IF NOT EXISTS " + index_name + " " + index_columns + " TYPE " + index_type;
        auto io = executeQuery(add_index_query, context, QueryFlags{.internal = true}).second;
        if (io.pipeline.initialized())
        {
            CompletedPipelineExecutor executor(io.pipeline);
            executor.execute();
        }
    }
    catch (Exception & e)
    {
        LOG_ERROR(getLogger("IndexManager"), "Error adding index: {}", e.message());
        return false;
    }

    String materialize_index_query
        = "ALTER TABLE " + table + " MATERIALIZE INDEX " + index_name;
    auto io = executeQuery(materialize_index_query, context, QueryFlags{.internal = true}).second;
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
    context->setSetting("mutations_sync", prev_mutation_setting);
    return true;
}

void IndexManager::dropIndex(const String & index_name)
{
    // Make sure the index will be dropped synchronously
    auto prev_mutation_setting = context->getSettingsRef()[Setting::mutations_sync].value;
    context->setSetting("mutations_sync", 2);

    String drop_index_query
        = "ALTER TABLE " + table + " DROP INDEX " + index_name;
    auto io = executeQuery(drop_index_query, context, QueryFlags{.internal = true}).second;
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }

    context->setSetting("mutations_sync", prev_mutation_setting);
}

Int64 IndexManager::estimate()
{
    createViews(context, workload.getCreateViews());
    UInt64 sum_read = 0;
    for (const auto & query : workload.getWorkload())
    {
        ParserQuery parser(query.c_str() + query.size());
        auto query_ast = parseQuery(
            parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        if (query_ast->as<ASTCreateQuery>() || query_ast->as<ASTSetQuery>() || query_ast->as<ASTDropQuery>())
        {
            if (query_ast->as<ASTSetQuery>()) {
                executeQuery(query, context, QueryFlags{.internal = true});
            }
            continue;
        }
        sum_read += getReadMarks(query, context);
    }
    dropViews(context, workload.getDropViews());
    return sum_read;
}

}
