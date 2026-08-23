#include <DataTypes/validateGroupByKeyType.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

void validateGroupByKeyType(const DataTypePtr & key_type, bool allow_suspicious_types)
{
    if (allow_suspicious_types)
        return;

    auto check = [](const IDataType & type)
    {
        if (isDynamic(type) || isVariant(type))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Data types Variant/Dynamic are not allowed in GROUP BY keys, because it can lead to unexpected results. "
                "Consider using a subcolumn with a specific data type instead (for example 'column.Int64' or 'json.some.path.:Int64' if "
                "its a JSON path subcolumn) or casting this column to a specific data type. "
                "Set setting allow_suspicious_types_in_group_by = 1 in order to allow it");
    };

    check(*key_type);
    key_type->forEachChild(check);
}

void validateWindowKeyType(const DataTypePtr & key_type, std::string_view clause)
{
    /// A window is formed by sorting, and aggregate function states are not comparable:
    /// ColumnAggregateFunction::compareAt reports every pair equal.
    if (hasAggregateFunctionType(key_type))
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Data type {} is not allowed in window {} keys, because it contains an aggregate function state "
            "whose values are not comparable",
            key_type->getName(),
            clause);
}

}
