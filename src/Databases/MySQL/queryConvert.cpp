#include <Databases/MySQL/queryConvert.h>

#include <IO/Operators.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr getFormattedOrderByExpression(const MySQLTableStruct & table_struct)
{
    if (table_struct.primary_keys.empty())
        return makeASTFunction("tuple");

    /// TODO: support unique key & key
    const auto function = std::make_shared<ASTFunction>();
    function->name = "tuple";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    function->arguments->children = table_struct.primary_keys;
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

ASTPtr getFormattedPartitionByExpression(const MySQLTableStruct & table_struct, const Context & context, size_t max_ranges, size_t min_rows_pre_range)
{
    ASTPtr partition_columns = std::make_shared<ASTExpressionList>();

    if (!table_struct.partition_keys.empty())
        partition_columns->children = table_struct.partition_keys;
    else if (!table_struct.primary_keys.empty())
    {
        ASTPtr expr_list = std::make_shared<ASTExpressionList>();
        expr_list->children = table_struct.primary_keys;

        auto syntax = SyntaxAnalyzer(context).analyze(expr_list, table_struct.columns_name_and_type);
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

String getUniqueColumnName(NamesAndTypesList columns_name_and_type, const String & prefix)
{
    const auto & is_unique = [&](const String & column_name)
    {
        for (const auto & column_name_and_type : columns_name_and_type)
        {
            if (column_name_and_type.name == column_name)
                return false;
        }

        return true;
    };

    if (is_unique(prefix))
        return prefix;

    for (size_t index = 0; ; ++index)
    {
        const String & cur_name = prefix + "_" + toString(index);
        if (is_unique(cur_name))
            return cur_name;
    }
}

String toCreateQuery(const MySQLTableStruct & table_struct, const Context & context)
{
    /// TODO: settings
    String create_version = getUniqueColumnName(table_struct.columns_name_and_type, "_create_version");
    String delete_version = getUniqueColumnName(table_struct.columns_name_and_type, "_delete_version");
    WriteBufferFromOwnString out;
    out << "CREATE TABLE " << (table_struct.if_not_exists ? "IF NOT EXISTS" : "")
        << backQuoteIfNeed(table_struct.database_name) + "." << backQuoteIfNeed(table_struct.table_name) << "("
        << queryToString(InterpreterCreateQuery::formatColumns(table_struct.columns_name_and_type))
        << ", " << create_version << " UInt64, " << delete_version << " UInt64" << ") ENGINE = MergeTree()"
        << " PARTITION BY " << queryToString(getFormattedPartitionByExpression(table_struct, context, 1000, 50000))
        << " ORDER BY " << queryToString(getFormattedOrderByExpression(table_struct));
    return out.str();
}

}
