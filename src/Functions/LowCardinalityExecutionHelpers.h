#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace LowCardinalityExecutionHelpers
{

/// Returns false if the constant value is not present in the dictionary. If the constant is NULL,
/// returns true and sets [dictionary_index] to the default LC null index, matching the existing
/// Array(LowCardinality) index-function behavior.
/// Keep this inlined: the Array(LowCardinality) index functions are sensitive to this setup codegen.
inline __attribute__((always_inline)) bool dictionaryIndexForConstant(
    const ColumnLowCardinality & low_cardinality_data,
    const ColumnPtr & value_column,
    const DataTypePtr & value_type,
    const DataTypePtr & target_type,
    UInt64 & dictionary_index)
{
    dictionary_index = 0;

    auto value = recursiveRemoveLowCardinality(value_column);
    if (value->isNullAt(0))
        return true;

    auto value_type_without_low_cardinality = recursiveRemoveLowCardinality(value_type);
    value = castColumn({value, value_type_without_low_cardinality, ""}, target_type);

    if (value->isNullable())
        value = assert_cast<const ColumnNullable &>(*value).getNestedColumnPtr();

    std::string_view elem = value->getDataAt(0);
    if (auto maybe_index = low_cardinality_data.getDictionary().getOrFindValueIndex(elem))
    {
        dictionary_index = *maybe_index;
        return true;
    }

    return false;
}

}

}
