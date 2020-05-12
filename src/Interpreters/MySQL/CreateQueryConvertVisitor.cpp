#include <Interpreters/MySQL/CreateQueryConvertVisitor.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/MySQL/ASTDeclareIndex.h>
#include <Parsers/MySQL/ASTDeclareOption.h>
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

static String convertDataType(const String & type_name, const ASTPtr & arguments, bool is_unsigned)
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
    {
        if (!arguments)
            return "Decimal(10, 0)";
        else if (arguments->children.size() == 1)
            return "Decimal(" + queryToString(arguments) + ", 0)";
        else if (arguments->children.size() == 2)
            return "Decimal(" + queryToString(arguments) + ")";
        else
            throw Exception("Decimal data type family must have exactly two arguments: precision and scale", ErrorCodes::UNKNOWN_TYPE);
    }


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

static String convertDataType(const String & type_name, const ASTPtr & arguments, bool is_unsigned, bool is_nullable)
{
    return (is_nullable ? "Nullable(" : "") + convertDataType(type_name, arguments, is_unsigned) + (is_nullable ? ")" : "");
}

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

    auto expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children = data.primary_keys;

    data.out << "CREATE TABLE " << (create.if_not_exists ? "IF NOT EXISTS" : "")
        << (create.database.empty() ? "" : backQuoteIfNeed(create.database) + ".") << backQuoteIfNeed(create.table)
        << "(" << queryToString(InterpreterCreateQuery::formatColumns(data.columns_name_and_type)) << ") ENGINE = MergeTree()"
        " PARTITION BY " << queryToString(data.getFormattedPartitionByExpression())
        << " ORDER BY " << queryToString(data.getFormattedOrderByExpression());
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

    bool is_nullable = true, is_unsigned = false;
    if (declare_column.column_options)
    {
        if (MySQLParser::ASTDeclareOptions * options = declare_column.column_options->as<MySQLParser::ASTDeclareOptions>())
        {
            if (options->changes.count("is_null"))
                is_nullable = options->changes["is_null"]->as<ASTLiteral>()->value.safeGet<UInt64>();

            if (options->changes.count("primary_key"))
                data.addPrimaryKey(std::make_shared<ASTIdentifier>(declare_column.name));

            if (options->changes.count("is_unsigned"))
                is_unsigned = options->changes["is_unsigned"]->as<ASTLiteral>()->value.safeGet<UInt64>();
            else if (options->changes.count("zero_fill"))
                is_unsigned = options->changes["zero_fill"]->as<ASTLiteral>()->value.safeGet<UInt64>();
        }
    }

    if (ASTFunction * function = declare_column.data_type->as<ASTFunction>())
        data.columns_name_and_type.emplace_back(declare_column.name,
            DataTypeFactory::instance().get(convertDataType(Poco::toUpper(function->name), function->arguments, is_unsigned, is_nullable)));
    else if (ASTIdentifier * identifier = declare_column.data_type->as<ASTIdentifier>())
        data.columns_name_and_type.emplace_back(declare_column.name,
            DataTypeFactory::instance().get(convertDataType(Poco::toUpper(identifier->name), ASTPtr{}, is_unsigned, is_nullable)));
    else
        throw Exception("Unsupported MySQL data type " + queryToString(declare_column.data_type) + ".", ErrorCodes::NOT_IMPLEMENTED);
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

ASTPtr CreateQueryMatcher::Data::getFormattedOrderByExpression()
{
    if (primary_keys.empty())
        return makeASTFunction("tuple");

    /// TODO: support unique key & key
    const auto function = std::make_shared<ASTFunction>();
    function->name = "tuple";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    function->arguments->children = primary_keys;

    return function;
}

template <typename TType>
Field choiceBetterRangeSize(TType min, TType max, size_t max_ranges, size_t min_size_pre_range)
{
    UInt64 interval = UInt64(max) - min;
    size_t calc_rows_pre_range = std::ceil(interval / double(max_ranges));
    size_t rows_pre_range = std::max(min_size_pre_range, calc_rows_pre_range);

    if (rows_pre_range >= interval)
        return Null();

    return rows_pre_range > std::numeric_limits<TType>::max() ? Field(UInt64(rows_pre_range)) : Field(TType(rows_pre_range));
}

ASTPtr CreateQueryMatcher::Data::getFormattedPartitionByExpression()
{
    ASTPtr partition_columns = std::make_shared<ASTExpressionList>();

    if (!partition_keys.empty())
        partition_columns->children = partition_keys;
    else if (!primary_keys.empty())
    {
        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        expr_list->children = primary_keys;

        auto syntax = SyntaxAnalyzer(context).analyze(expr_list, columns_name_and_type);
        auto index_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);
        const NamesAndTypesList & required_names_and_types = index_expr->getRequiredColumnsWithTypes();

        const auto & addPartitionColumn = [&](const String & column_name, const DataTypePtr & type, Field better_pre_range_size)
        {
            partition_columns->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));

            if (type->isNullable())
                partition_columns->children.back() = makeASTFunction("assumeNotNull", partition_columns->children.back());

            if (!better_pre_range_size.isNull())
                partition_columns->children.back()
                    = makeASTFunction("divide", partition_columns->children.back(), std::make_shared<ASTLiteral>(better_pre_range_size));
        };

        for (const auto & required_name_and_type : required_names_and_types)
        {
            DataTypePtr assume_not_null = required_name_and_type.type;
            if (assume_not_null->isNullable())
                assume_not_null = (static_cast<const DataTypeNullable &>(*assume_not_null)).getNestedType();

            WhichDataType which(assume_not_null);
            if (which.isInt8())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<Int8>(
                    std::numeric_limits<Int8>::min(), std::numeric_limits<Int8>::max(), max_ranges, min_rows_pre_range));
            else if (which.isInt16())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<Int16>(
                    std::numeric_limits<Int16>::min(), std::numeric_limits<Int16>::max(), max_ranges, min_rows_pre_range));
            else if (which.isInt32())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<Int32>(
                    std::numeric_limits<Int32>::min(), std::numeric_limits<Int32>::max(), max_ranges, min_rows_pre_range));
            else if (which.isInt64())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<Int64>(
                    std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max(), max_ranges, min_rows_pre_range));
            else if (which.isUInt8())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<UInt8>(
                    std::numeric_limits<UInt8>::min(), std::numeric_limits<UInt8>::max(), max_ranges, min_rows_pre_range));
            else if (which.isUInt16())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<UInt16>(
                    std::numeric_limits<UInt16>::min(), std::numeric_limits<UInt16>::max(), max_ranges, min_rows_pre_range));
            else if (which.isUInt32())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<UInt32>(
                    std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max(), max_ranges, min_rows_pre_range));
            else if (which.isUInt64())
                addPartitionColumn(required_name_and_type.name, required_name_and_type.type, choiceBetterRangeSize<UInt64>(
                    std::numeric_limits<UInt64>::min(), std::numeric_limits<UInt64>::max(), max_ranges, min_rows_pre_range));
            else if (which.isDateOrDateTime())
            {
                partition_columns->children.emplace_back(std::make_shared<ASTIdentifier>(required_name_and_type.name));

                if (required_name_and_type.type->isNullable())
                    partition_columns->children.back() = makeASTFunction("assumeNotNull", partition_columns->children.back());

                partition_columns->children.back() = makeASTFunction("toYYYYMM", partition_columns->children.back());
            }
        }
    }

    const auto function = std::make_shared<ASTFunction>();
    function->name = "tuple";
    function->arguments = partition_columns;
    function->children.push_back(function->arguments);
    return function;
}

}

}
