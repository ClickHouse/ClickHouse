#include <Interpreters/MySQL/CreateQueryConvertVisitor.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
#include <Poco/String.h>

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
    if (auto * t = ast->as<ASTCreateQuery>())
        visit(*t, ast, data);
}
void CreateQueryMatcher::visit(ASTCreateQuery & create, ASTPtr & /*ast*/, Data & data)
{
    if (create.like_table)
        throw Exception("Cannot convert create like statement to ClickHouse SQL", ErrorCodes::NOT_IMPLEMENTED);

    data.out << "CREATE TABLE " << (create.if_not_exists ? "IF NOT EXISTS" : "")
             << (create.database.empty() ? "" : backQuoteIfNeed(create.database) + ".") << backQuoteIfNeed(create.table) << "(";

    if (create.columns_list)
        visitColumns(*create.columns_list->as<ASTCreateDefines>(), create.columns_list, data);

    data.out << ") ENGINE = MergeTree()";
}

void CreateQueryMatcher::visitColumns(ASTCreateDefines & create_defines, ASTPtr & /*ast*/, Data & data)
{
    if (!create_defines.columns || create_defines.columns->children.empty())
        throw Exception("Missing definition of columns.", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

    bool is_first = true;
    for (auto & column : create_defines.columns->children)
    {
        if (!is_first)
            data.out << ",";

        is_first = false;
        visitColumns(*column->as<ASTDeclareColumn>(), column, data);
    }
}

static String convertDataType(const String & type_name, const ASTPtr & arguments, bool is_unsigned, bool /*is_national*/)
{
    if (type_name == "TINYINT")
        return is_unsigned ? "UInt8" : "Int8";
    else if (type_name == "BOOL" || type_name == "BOOLEAN")
        return "UInt8";
    else if (type_name == "SMALLINT")
        return is_unsigned ? "UInt16" : "Int16";
    else if (type_name == "INT" || type_name == "MEDIUMINT" || type_name == "INTEGER")
        return is_unsigned ? "UInt32" : "Int32";
    else if (type_name == "BIGINT")
        return is_unsigned ? "UInt64" : "Int64";
    else if (type_name == "FLOAT")
        return "Float32";
    else if (type_name == "DOUBLE" || type_name == "PRECISION" || type_name == "REAL")
        return "Float64";
    else if (type_name == "DECIMAL" || type_name == "DEC" || type_name == "NUMERIC" || type_name == "FIXED")
        return arguments ? "Decimal(" + queryToString(arguments) + ")" : "Decimal(10, 0)";

    if (type_name == "DATE")
        return "Date";
    else if (type_name == "DATETIME" || type_name == "TIMESTAMP")
        return "DateTime";
    else if (type_name == "TIME")
        return "DateTime64";
    else if (type_name == "YEAR")
        return "Int16";

    if (type_name == "BINARY")
        return arguments ? "FixedString(" + queryToString(arguments) + ")" : "FixedString(1)";

    return "String";
}

void CreateQueryMatcher::visitColumns(const ASTDeclareColumn & declare_column, ASTPtr &, Data & data)
{
    data.out << declare_column.name << " ";

    if (!declare_column.data_type)
        throw Exception("Missing type in definition of column.", ErrorCodes::UNKNOWN_TYPE);

    bool is_unsigned = false;
    bool is_nullable = true;
    bool is_national = false;
    if (declare_column.column_options)
    {
        if (ASTDeclareOptions * options = declare_column.column_options->as<ASTDeclareOptions>())
        {
            if (options->changes.count("is_null"))
                is_nullable = options->changes["is_null"]->as<ASTLiteral>()->value.safeGet<UInt64>();

            if (options->changes.count("is_national"))
                is_national = options->changes["is_national"]->as<ASTLiteral>()->value.safeGet<UInt64>();

            if (options->changes.count("is_unsigned"))
                is_unsigned = options->changes["is_unsigned"]->as<ASTLiteral>()->value.safeGet<UInt64>();
            else if (options->changes.count("zero_fill"))
                is_unsigned = options->changes["zero_fill"]->as<ASTLiteral>()->value.safeGet<UInt64>();
        }
    }

    if (ASTFunction * function = declare_column.data_type->as<ASTFunction>())
        data.out << (is_nullable ? "Nullable(" : "")
                 << convertDataType(Poco::toUpper(function->name), function->arguments, is_unsigned, is_national)
                 << (is_nullable ? ")" : "");

    if (ASTIdentifier * identifier = declare_column.data_type->as<ASTIdentifier>())
        data.out << (is_nullable ? "Nullable(" : "") << convertDataType(Poco::toUpper(identifier->name), ASTPtr{}, is_unsigned, is_national)
                 << (is_nullable ? ")" : "");
}

}

}
