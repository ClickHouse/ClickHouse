#include <IO/Operators.h>
#include <Interpreters/MySQL/CreateQueryVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Parsers/queryToString.h>
#include <Poco/String.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
}

namespace MySQLVisitor
{

void CreateQueryMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto * t = ast->as<MySQLParser::ASTCreateQuery>())
        visit(*t, ast, data);
}

void CreateQueryMatcher::visit(const MySQLParser::ASTCreateQuery & create, const ASTPtr &, Data & data)
{
    if (create.like_table)
        throw Exception("Cannot convert create like statement to ClickHouse SQL", ErrorCodes::NOT_IMPLEMENTED);

    if (create.columns_list)
        visit(*create.columns_list->as<MySQLParser::ASTCreateDefines>(), create.columns_list, data);

    if (create.partition_options)
        visit(*create.partition_options->as<MySQLParser::ASTDeclarePartitionOptions>(), create.partition_options, data);

    data.table_name = create.table;
    data.database_name = create.database;
    data.if_not_exists = create.if_not_exists;
}

void CreateQueryMatcher::visit(const MySQLParser::ASTDeclareIndex & declare_index, const ASTPtr &, Data & data)
{
    if (startsWith(declare_index.index_type, "PRIMARY_KEY_"))
        data.addPrimaryKey(declare_index.index_columns);
}

void CreateQueryMatcher::visit(const MySQLParser::ASTCreateDefines & create_defines, const ASTPtr &, Data & data)
{
    if (!create_defines.columns || create_defines.columns->children.empty())
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    if (create_defines.indices)
    {
        for (auto & index : create_defines.indices->children)
            visit(*index->as<MySQLParser::ASTDeclareIndex>(), index, data);
    }

    for (auto & column : create_defines.columns->children)
        visit(*column->as<MySQLParser::ASTDeclareColumn>(), column, data);
}

void CreateQueryMatcher::visit(const MySQLParser::ASTDeclareColumn & declare_column, const ASTPtr &, Data & data)
{
    if (!declare_column.data_type)
        throw Exception("Missing type in definition of column.", ErrorCodes::UNKNOWN_TYPE);

    bool is_nullable = true;
    if (declare_column.column_options)
    {
        if (MySQLParser::ASTDeclareOptions * options = declare_column.column_options->as<MySQLParser::ASTDeclareOptions>())
        {
            if (options->changes.count("is_null"))
                is_nullable = options->changes["is_null"]->as<ASTLiteral>()->value.safeGet<UInt64>();

            if (options->changes.count("primary_key"))
                data.addPrimaryKey(std::make_shared<ASTIdentifier>(declare_column.name));

         /*   if (options->changes.count("is_unsigned"))
                is_unsigned = options->changes["is_unsigned"]->as<ASTLiteral>()->value.safeGet<UInt64>();
            else if (options->changes.count("zero_fill"))
                is_unsigned = options->changes["zero_fill"]->as<ASTLiteral>()->value.safeGet<UInt64>();*/
        }
    }

    data.columns_name_and_type.emplace_back(declare_column.name, DataTypeFactory::instance().get(
        (is_nullable ? "Nullable(" : "") + queryToString(declare_column.data_type) + (is_nullable ? ")" : "")));
}

void CreateQueryMatcher::visit(const MySQLParser::ASTDeclarePartitionOptions & declare_partition_options, const ASTPtr &, Data & data)
{
    data.addPartitionKey(declare_partition_options.partition_expression);
}

void CreateQueryMatcher::Data::addPrimaryKey(const ASTPtr & primary_key)
{
    if (const auto & function_index_columns = primary_key->as<ASTFunction>())
    {
        if (function_index_columns->name != "tuple")
            throw Exception("Unable to parse function primary key from MySQL.", ErrorCodes::NOT_IMPLEMENTED);

        for (const auto & index_column : function_index_columns->arguments->children)
            primary_keys.emplace_back(index_column);
    }
    else if (const auto & expression_index_columns = primary_key->as<ASTExpressionList>())
    {
        for (const auto & index_column : expression_index_columns->children)
            primary_keys.emplace_back(index_column);
    }
    else
        primary_keys.emplace_back(primary_key);
}

void CreateQueryMatcher::Data::addPartitionKey(const ASTPtr & partition_key)
{
    if (const auto & function_partition_columns = partition_key->as<ASTFunction>())
    {
        if (function_partition_columns->name != "tuple")
            throw Exception("Unable to parse function partition by from MySQL.", ErrorCodes::NOT_IMPLEMENTED);

        for (const auto & partition_column : function_partition_columns->arguments->children)
            partition_keys.emplace_back(partition_column);
    }
    else if (const auto & expression_partition_columns = partition_key->as<ASTExpressionList>())
    {
        for (const auto & partition_column : expression_partition_columns->children)
            partition_keys.emplace_back(partition_column);
    }
    else
        partition_keys.emplace_back(partition_key);
}

}

bool MySQLTableStruct::operator==(const MySQLTableStruct & other) const
{
    const auto & this_expression = std::make_shared<ASTExpressionList>();
    this_expression->children.insert(this_expression->children.begin(), primary_keys.begin(), primary_keys.end());
    this_expression->children.insert(this_expression->children.begin(), partition_keys.begin(), partition_keys.end());

    const auto & other_expression = std::make_shared<ASTExpressionList>();
    other_expression->children.insert(other_expression->children.begin(), other.primary_keys.begin(), other.primary_keys.end());
    other_expression->children.insert(other_expression->children.begin(), other.partition_keys.begin(), other.partition_keys.end());

    return queryToString(this_expression) == queryToString(other_expression) && columns_name_and_type == other.columns_name_and_type;
}

MySQLTableStruct visitCreateQuery(ASTPtr & create_query, const Context & context, const std::string & new_database)
{
    create_query->as<MySQLParser::ASTCreateQuery>()->database = new_database;
    MySQLVisitor::CreateQueryVisitor::Data table_struct(context);
    MySQLVisitor::CreateQueryVisitor visitor(table_struct);
    visitor.visit(create_query);
    return std::move(table_struct);
}

MySQLTableStruct visitCreateQuery(const String & create_query, const Context & context, const std::string & new_database)
{
    MySQLParser::ParserCreateQuery p_create_query;
    ASTPtr ast_create_query = parseQuery(p_create_query, create_query.data(), create_query.data() + create_query.size(), "", 0, 0);

    if (!ast_create_query || !ast_create_query->as<MySQLParser::ASTCreateQuery>())
        throw Exception("LOGICAL ERROR: ast cannot cast to MySQLParser::ASTCreateQuery.", ErrorCodes::LOGICAL_ERROR);

    return visitCreateQuery(ast_create_query, context, new_database);
}

}
