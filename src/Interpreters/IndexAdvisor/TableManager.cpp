#include "TableManager.h"
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

#include <boost/algorithm/string/split.hpp>


namespace DB
{

namespace Setting
{
extern const SettingsDouble sampling_proportion;
}
namespace
{

String replaceTableNames(const String & input, const String & table, const String & estimation_table)
{
    String result = input;
    String pattern = "\\b" + table + "\\b";
    re2::RE2 re(pattern, re2::RE2::Quiet);
    if (!re.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid regex: {}", pattern);

    RE2::GlobalReplace(&result, re, estimation_table);
    LOG_INFO(getLogger("TableManager"), "Replaced table name {} with {} in {}", table, estimation_table, result);
    return result;
}


std::vector<String> extractColumnsFromCreateDDL(const String & ddl_sql, ASTPtr & out_ast)
{
    ParserCreateQuery parser;
    out_ast
        = parseQuery(parser, ddl_sql, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    auto * create_ast = out_ast->as<ASTCreateQuery>();
    if (!create_ast || !create_ast->columns_list || !create_ast->columns_list->columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse CREATE TABLE or missing columns");

    std::vector<String> cols;
    for (const auto & col_ptr : create_ast->columns_list->columns->children)
    {
        const auto * col_decl = col_ptr->as<ASTColumnDeclaration>();
        if (col_decl)
            cols.push_back(col_decl->name);
    }
    return cols;
}

String getTableDDL(const String & table, ContextMutablePtr context)
{
    BlockIO io_show = executeQuery("SHOW CREATE TABLE " + table, context, QueryFlags{.internal = true}).second;
    io_show.pipeline.setNumThreads(1);
    PullingPipelineExecutor show_exec(io_show.pipeline);
    String ddl;
    Block block;
    if (show_exec.pull(block))
    {
        auto col = block.getByPosition(0).column;
        const auto * col_str = typeid_cast<const ColumnString *>(col.get());
        if (!col_str)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnString");
        }

        ddl = col_str->getDataAt(0).toString();
    }
    return ddl;
}

void createTableQuery(ContextMutablePtr context, const String & table, const String & pk_columns, ASTPtr & create_ast)
{
    String columns_def;
    const auto * create_ast_ptr = create_ast->as<ASTCreateQuery>();
    if (!create_ast_ptr || !create_ast_ptr->columns_list || !create_ast_ptr->columns_list->columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to parse columns from CREATE TABLE AST");

    for (size_t i = 0; i < create_ast_ptr->columns_list->columns->children.size(); ++i)
    {
        if (i > 0)
            columns_def += ", ";
        columns_def += create_ast_ptr->columns_list->columns->children[i]->formatForErrorMessage();
    }
    String query_for_create_target_table = "CREATE TABLE " + table + " (" + columns_def + ") ENGINE = MergeTree() ORDER BY " + pk_columns;
    LOG_INFO(getLogger("TableManager"), "Create table query: {}", query_for_create_target_table);

    executeQuery(query_for_create_target_table, context, QueryFlags{.internal = true});
}

void insertSample(const String & table, const String & estimation_table, ContextMutablePtr context)
{
    LOG_INFO(getLogger("TableManager"), "Inserting sample for table {}", table);
    UInt64 sample_size = static_cast<UInt64>(context->getSettingsRef()[Setting::sampling_proportion] * 1000);
    String hash_expr = "rand() % 1000 < " + std::to_string(sample_size);

    auto settings = context->getSettingsRef();
    settings.set("async_insert", false);
    context->setSettings(settings);

    String insert_query = "INSERT INTO " + estimation_table + " SELECT * FROM " + table + " WHERE " + hash_expr;
    auto io = executeQuery(insert_query, context, QueryFlags{.internal = true}).second;
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
        LOG_INFO(getLogger("TableManager"), "Inserted sample for table {}", table);
    }
    else
    {
        LOG_INFO(getLogger("TableManager"), "Pipeline not initialized, insert may be asynchronous");
    }
}

void insertWithLimit(const String & table, const String & estimation_table, ContextMutablePtr context)
{
    LOG_INFO(getLogger("TableManager"), "Inserting with limit for table {}", table);
    String query = "INSERT INTO " + estimation_table + " SELECT * FROM " + table + " LIMIT 10000";
    auto io = executeQuery(query, context, QueryFlags{.internal = true}).second;
    io.pipeline.setNumThreads(1);
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
        LOG_INFO(getLogger("TableManager"), "Inserted with limit for table {}", table);
    }
    else
    {
        LOG_INFO(getLogger("TableManager"), "Pipeline not initialized, insert may be asynchronous");
    }
}

bool isTableEmpty(const String & table, ContextMutablePtr context)
{
    String query = "SELECT count() FROM " + table;
    auto io = executeQuery(query, context, QueryFlags{.internal = true}).second;
    io.pipeline.setNumThreads(1);
    PullingPipelineExecutor exec(io.pipeline);
    Block block;
    if (exec.pull(block))
    {
        LOG_INFO(getLogger("TableManager"), "Table {} has {} rows", table, block.getByPosition(0).column->getUInt(0));
        return block.getByPosition(0).column->getUInt(0) == 0;
    }
    return true;
}

UInt64 getReadMarks(const String & query, ContextMutablePtr context)
{
    auto io_exp = executeQuery("EXPLAIN ESTIMATE " + query, context, QueryFlags{.internal = true}).second;
    io_exp.pipeline.setNumThreads(1);
    PullingPipelineExecutor exec(io_exp.pipeline);
    Block block;
    UInt64 sum_read = 0;
    while (exec.pull(block))
    {
        const auto & col = block.getByPosition(4);
        for (size_t j = 0; j < col.column->size(); ++j)
        {
            LOG_INFO(getLogger("TableManager"), "Read marks: {} {}", block.getByPosition(1).column->getDataAt(j).toString(), block.getByPosition(4).column->getUInt(j));
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

UInt64 TableManager::estimate(const String & pk_columns)
{
    if (table.empty() || pk_columns.empty())
    {
        return UINT64_MAX;
    }
    createViews(context, workload.getCreateViews());
    buildTable(pk_columns);
    auto result = estimateQueries();
    dropViews(context, workload.getDropViews());
    dropTable(estimation_table);
    return result;
}

void TableManager::dropTable(const String & target_table)
{
    executeQuery("DROP TABLE IF EXISTS " + target_table, context, QueryFlags{.internal = true});
}

void TableManager::buildTable(const String & pk_columns)
{
    String ddl = getTableDDL(table, context);
    if (ddl.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to get DDL for table {}", table);
    LOG_INFO(getLogger("TableManager"), "DDL: {}", ddl);

    ASTPtr create_ast;
    std::vector<String> all_cols = extractColumnsFromCreateDDL(ddl, create_ast);
    if (all_cols.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns found in CREATE TABLE AST");
    
    String all_cols_str;
    for (const auto & col : all_cols)
    {
        all_cols_str += col + ", ";
    }
    LOG_INFO(getLogger("TableManager"), "All columns: {}", all_cols_str);

    dropTable(estimation_table);
    createTableQuery(context, estimation_table, pk_columns, create_ast);
    insertSample(table, estimation_table, context);
    if (isTableEmpty(estimation_table, context))
    {
        insertWithLimit(table, estimation_table, context);
    }
    if (isTableEmpty(estimation_table, context))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is still empty after inserting with limit", estimation_table);
    }
}

UInt64 TableManager::estimateQueries()
{
    UInt64 sum_read = 0;
    for (const auto & query : workload.getWorkload())
    {
        ParserQuery parser(query.c_str() + query.size());
        auto query_ast = parseQuery(
            parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        // LOG_DEBUG(getLogger("TableManager"), "Query ast type: {}", (query_ast->as<ASTSetQuery>() ? "ASTSetQuery" : (query_ast->as<ASTCreateQuery>() ? "ASTCreateQuery" : (query_ast->as<ASTDropQuery>() ? "ASTDropQuery" : "Unknown"))));
        if (query_ast->as<ASTCreateQuery>() || query_ast->as<ASTSetQuery>() || query_ast->as<ASTDropQuery>())
        {
            if (query_ast->as<ASTSetQuery>()) {
                executeQuery(query, context, QueryFlags{.internal = true});
            }
            // LOG_DEBUG(getLogger("TableManager"), "No need to estimate query: {}", query);
            continue;
        }
        String new_query = replaceTableNames(query, table, estimation_table);
        LOG_DEBUG(getLogger("TableManager"), "New query: {}", new_query);
        sum_read += getReadMarks(new_query, context);
        LOG_DEBUG(getLogger("TableManager"), "Estimated query: {} with read marks: {}", query, sum_read);
    }
    return sum_read;
}

}
