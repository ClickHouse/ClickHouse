#include "TableManager.h"
#include <Interpreters/Context_fwd.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <boost/algorithm/string/split.hpp>
#include <Core/Settings.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTDropQuery.h>

namespace DB
{

namespace Setting
{
    extern const SettingsDouble sampling_proportion;
}

const String ESTIMATION_SUFFIX = "_estimation";

String replaceTableNames(
    const String & input,
    const std::unordered_map<String, String> & replacements)
{
    String result = input;
    for (const auto & [key, val] : replacements)
    {
        // Use word boundaries to ensure only whole table names are replaced
        String pattern = "\\b" + key + "\\b";
        re2::RE2 re(pattern, re2::RE2::Quiet);
        if (!re.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid regex: {}", pattern);

        RE2::GlobalReplace(&result, re, val);
        LOG_INFO(getLogger("TableManager"), "Replaced table name {} with {} in {}", key, val, result);
    }
    return result;
}

UInt64 TableManager::estimate(std::unordered_map<String, Strings> & pk_columns) {
    for (const auto & table : workload.getTables())
    {
        if (!table.empty() && !pk_columns[table].empty())
            buildTable(table, pk_columns[table]);
    }
    return estimateQueries();
}

static std::vector<String> extractColumnsFromCreateDDL(const String & ddl_sql, ASTPtr & out_ast)
{
    ParserCreateQuery parser;
    out_ast = parseQuery(parser, ddl_sql, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
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

void TableManager::buildTable(const String& table, const Strings & pk_columns)
{
    String target_table = table + ESTIMATION_SUFFIX;
    BlockIO io_show = executeQuery("SHOW CREATE TABLE " + table, context, QueryFlags{ .internal = true }).second;
    io_show.pipeline.setNumThreads(1);
    PullingPipelineExecutor show_exec(io_show.pipeline);
    String ddl;
    {
        Block block;
        if (show_exec.pull(block))
        {
            auto col = block.getByPosition(0).column;
            const auto * col_str = typeid_cast<const ColumnString *>(col.get());
            if (!col_str) {

                // LOG_INFO(getLogger("TableManager"), "Expected ColumnString");
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnString");
            }

            ddl = col_str->getDataAt(0).toString();
        }
    }

    ASTPtr create_ast;
    std::vector<String> all_cols = extractColumnsFromCreateDDL(ddl, create_ast);
    if (all_cols.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns found in CREATE TABLE AST");

    UInt64 sample_size = static_cast<UInt64>(context->getSettingsRef()[Setting::sampling_proportion] * 1000);
    String hash_expr = "rand() % 1000 < " + std::to_string(sample_size);

    String pk_expr = "(" + pk_columns[0];
    for (size_t i = 1; i < pk_columns.size(); ++i)
        pk_expr += ", " + pk_columns[i];
    pk_expr += ")";

    // LOG_INFO(getLogger("TableManager"), "DDL is {}", ddl);
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

    executeQuery("DROP TABLE IF EXISTS " + target_table, context, QueryFlags{ .internal = true });
    String query_for_create_target_table = "CREATE TABLE " + target_table +
        " (" + columns_def + ") ENGINE = MergeTree() ORDER BY " + pk_expr;
    // LOG_INFO(getLogger("TableManager"), "Creating table with query: {}", query_for_create_target_table);
    executeQuery(query_for_create_target_table, context, QueryFlags{ .internal = true });

    String query_for_insert_into_target_table = "WITH y as (SELECT * FROM " + table + " WHERE " + hash_expr + ")" + 
        " INSERT INTO " + target_table + " SELECT * FROM y";
    // LOG_INFO(getLogger("TableManager"), "Insert table: {}", query_for_insert_into_target_table);
    executeQuery(query_for_insert_into_target_table, context, QueryFlags{ .internal = true });
    replacement_tables[table] = target_table;
    //TODO: check twice?
}

UInt64 TableManager::estimateQueries() 
{
    UInt64 sum_read  = 0;
    for (const auto & q : workload.getWorkload())
    {
        String new_query = replaceTableNames(q, replacement_tables);
        BlockIO io_exp;
        ParserQuery parser(new_query.c_str() + new_query.size());
        auto query_ast = parseQuery(parser, new_query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        if (query_ast->as<ASTCreateQuery>() || query_ast->as<ASTSetQuery>() || query_ast->as<ASTDropQuery>())
        {
            // LOG_INFO(getLogger("TableManager"), "Skipping query: {}", new_query);
            executeQuery(new_query, context, QueryFlags{ .internal = true });
            continue;
        }
        else
        {
            // LOG_INFO(getLogger("TableManager"), "Estimating query: {}", new_query);
            io_exp = executeQuery("EXPLAIN ESTIMATE " + new_query, context, QueryFlags{ .internal = true }).second;
            io_exp.pipeline.setNumThreads(1);
        }
        // io_exp.pipeline.init();
        PullingPipelineExecutor exec(io_exp.pipeline);

        Block block;
        while (exec.pull(block))
        {
            const auto & col = block.getByPosition(4);
            // LOG_INFO(getLogger("TableManager"), "Column block: {}", col.column->dumpStructure());
            for (size_t j= 0; j < col.column->size(); ++j)
            {
                // LOG_INFO(getLogger("TableManager"), "Column block value {}: {}", j, col.column->getDataAt(j).toString());
                sum_read += col.column->getUInt(j);
            }
        }
        // LOG_INFO(getLogger("TableManager"), "Read: {}", sum_read);
    }
    return sum_read;
}

}
