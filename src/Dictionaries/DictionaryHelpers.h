#pragma once

#include <Columns/IColumn.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include "DictionaryStructure.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

/**
 * In Dictionaries implementation String attribute is stored in arena and StringRefs are pointing to it.
 */
template <typename DictionaryAttributeType>
using DictionaryValueType =
    std::conditional_t<std::is_same_v<DictionaryAttributeType, String>, StringRef, DictionaryAttributeType>;

/**
 * Used to create column with right type for DictionaryAttributeType.
 */
template <typename DictionaryAttributeType>
class DictionaryAttributeColumnProvider
{
public:
    using ColumnType =
        std::conditional_t<std::is_same_v<DictionaryAttributeType, String>, ColumnString,
            std::conditional_t<IsDecimalNumber<DictionaryAttributeType>, ColumnDecimal<DictionaryAttributeType>,
                ColumnVector<DictionaryAttributeType>>>;

    using ColumnPtr = typename ColumnType::MutablePtr;

    static ColumnPtr getColumn(const DictionaryAttribute & dictionary_attribute, size_t size)
    {
        if constexpr (std::is_same_v<DictionaryAttributeType, String>)
        {
            return ColumnType::create();
        }
        if constexpr (IsDecimalNumber<DictionaryAttributeType>)
        {
            auto scale = getDecimalScale(*dictionary_attribute.nested_type);
            return ColumnType::create(size, scale);
        }
        else if constexpr (IsNumber<DictionaryAttributeType>)
            return ColumnType::create(size);
        else
            throw Exception{"Unsupported attribute type.", ErrorCodes::TYPE_MISMATCH};
    }
};

/**
  * DictionaryDefaultValueExtractor used to simplify getting default value for IDictionary function `getColumn`.
  * Provides interface for getting default value with operator[];
  *
  * If default_values_column is null then attribute_default_value will be used.
  * If default_values_column is not null in constructor than this column values will be used as default values.
 */
template <typename DictionaryAttributeType>
class DictionaryDefaultValueExtractor
{
    using DefaultColumnType = typename DictionaryAttributeColumnProvider<DictionaryAttributeType>::ColumnType;

public:
    using DefaultValueType = DictionaryValueType<DictionaryAttributeType>;

    DictionaryDefaultValueExtractor(DictionaryAttributeType attribute_default_value, ColumnPtr default_values_column_ = nullptr)
        : default_value(std::move(attribute_default_value))
    {
        if (default_values_column_ == nullptr)
            use_default_value_from_column = false;
        else
        {
            if (const auto * const default_col = checkAndGetColumn<DefaultColumnType>(*default_values_column_))
            {
                default_values_column = default_col;
                use_default_value_from_column = true;
            }
            else if (const auto * const default_col_const = checkAndGetColumnConst<DefaultColumnType>(default_values_column_.get()))
            {
                default_value = default_col_const->template getValue<DictionaryAttributeType>();
                use_default_value_from_column = false;
            }
            else
                throw Exception{"Type of default column is not the same as dictionary attribute type.", ErrorCodes::TYPE_MISMATCH};
        }
    }

    DefaultValueType operator[](size_t row)
    {
        if (!use_default_value_from_column)
            return static_cast<DefaultValueType>(default_value);

        assert(default_values_column != nullptr);

        if constexpr (std::is_same_v<DefaultColumnType, ColumnString>)
            return default_values_column->getDataAt(row);
        else
            return default_values_column->getData()[row];
    }
private:
    DictionaryAttributeType default_value;
    const DefaultColumnType * default_values_column = nullptr;
    bool use_default_value_from_column = false;
};

/**
 * Returns ColumnVector data as PaddedPodArray.

 * If column is constant parameter backup_storage is used to store values.
 */
template <typename T>
static const PaddedPODArray<T> & getColumnVectorData(
    const IDictionaryBase * dictionary,
    const ColumnPtr column,
    PaddedPODArray<T> & backup_storage)
{
    bool is_const_column = isColumnConst(*column);
    auto full_column = column->convertToFullColumnIfConst();
    auto vector_col = checkAndGetColumn<ColumnVector<T>>(full_column.get());

    if (!vector_col)
    {
        throw Exception{ErrorCodes::TYPE_MISMATCH,
            "{}: type mismatch: column has wrong type expected {}",
            dictionary->getDictionaryID().getNameForLogs(),
            TypeName<T>::get()};
    }

    if (is_const_column)
    {
        // With type conversion and const columns we need to use backup storage here
        auto & data = vector_col->getData();
        backup_storage.assign(data);

        return backup_storage;
    }
    else
    {
        return vector_col->getData();
    }
}

}
