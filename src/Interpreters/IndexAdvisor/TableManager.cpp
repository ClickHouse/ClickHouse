#include "TableManager.h"
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Core/Settings.h>
#include <Databases/DDLRenamingVisitor.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

#include <boost/algorithm/string/split.hpp>


namespace DB
{

namespace Setting
{
extern const SettingsDouble index_advisor_sampling_proportion;
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
    return result;
}

ASTPtr replacePK(const String & ddl_sql, const String & pk_columns)
{
    ParserCreateQuery parser;
    ASTPtr out_ast = parseQuery(parser, ddl_sql, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    auto * create_ast = out_ast->as<ASTCreateQuery>();
    if (!create_ast || !create_ast->storage)
        return nullptr;

    ParserExpression parser_expr;
    ASTPtr expr_ast = parseQuery(parser_expr, pk_columns, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    create_ast->storage->set(create_ast->storage->primary_key, expr_ast);
    create_ast->storage->set(create_ast->storage->order_by, expr_ast);

    return out_ast;
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

void createTable(ContextMutablePtr context, ASTPtr & create_ast)
{
    InterpreterCreateQuery interpreter_create(create_ast, context);
    interpreter_create.setInternal(true);
    auto io = interpreter_create.execute();
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
    else
    {
        LOG_INFO(getLogger("TableManager"), "Failed to create table, pipeline not initialized");
    }
}

void insertSample(const String & table, const String & estimation_table, ContextMutablePtr context)
{
    UInt64 sample_size = static_cast<UInt64>(context->getSettingsRef()[Setting::index_advisor_sampling_proportion] * 1000);
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
    }
}

void insertWithLimit(const String & table, const String & estimation_table, ContextMutablePtr context)
{
    String query = "INSERT INTO " + estimation_table + " SELECT * FROM " + table + " LIMIT 10000";
    auto io = executeQuery(query, context, QueryFlags{.internal = true}).second;
    io.pipeline.setNumThreads(1);
    if (io.pipeline.initialized())
    {
        CompletedPipelineExecutor executor(io.pipeline);
        executor.execute();
    }
    else
    {
        LOG_INFO(getLogger("TableManager"), "Failed to insert with limit, pipeline not initialized");
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

} // namespace

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

    ASTPtr create_ast = replacePK(ddl, pk_columns);
    if (!create_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to replace PK in DDL for table {}", table);
    
    DDLRenamingMap renaming_map;
    QualifiedTableName old_table_name = {context->getCurrentDatabase(), table};
    QualifiedTableName new_table_name = {context->getCurrentDatabase(), estimation_table};
    renaming_map.setNewTableName(old_table_name, new_table_name);
    renameDatabaseAndTableNameInCreateQuery(create_ast, renaming_map, context);

    dropTable(estimation_table);
    createTable(context, create_ast);
    insertSample(table, estimation_table, context);
    if (isTableEmpty(estimation_table, context))
        insertWithLimit(table, estimation_table, context);
    if (isTableEmpty(estimation_table, context))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {} is still empty after inserting with limit", estimation_table);
}

UInt64 TableManager::estimateQueries()
{
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
        String new_query = replaceTableNames(query, table, estimation_table);
        auto read_marks = getReadMarks(new_query, context);
        sum_read += read_marks;
    }
    return sum_read;
}

}
