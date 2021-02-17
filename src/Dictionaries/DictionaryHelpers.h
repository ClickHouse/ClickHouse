#pragma once

#include <Common/Arena.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/DictionaryStructure.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

/** Support class for dictionary storages.

    The main idea is that during fetch we create all columns, but fill only columns that client requested.

    We need to create other columns during fetch, because in case of serialized storage we can skip
    unnecessary columns serialized in cache with skipSerializedInArena method.

    When result is fetched from the storage client of storage can filterOnlyNecessaryColumns
    and get only columns that match attributes_names_to_fetch.
 */
class DictionaryStorageFetchRequest
{
public:
    DictionaryStorageFetchRequest(const DictionaryStructure & structure, const Strings & attributes_names_to_fetch)
        : attributes_to_fetch_names_set(attributes_names_to_fetch.begin(), attributes_names_to_fetch.end())
        , attributes_to_fetch_filter(structure.attributes.size(), false)
    {
        size_t attributes_size = structure.attributes.size();
        attributes_to_fetch_types.reserve(attributes_size);

        for (size_t i = 0; i < attributes_size; ++i)
        {
            const auto & name = structure.attributes[i].name;
            const auto & type = structure.attributes[i].type;
            attributes_to_fetch_types.emplace_back(type);

            if (attributes_to_fetch_names_set.find(name) != attributes_to_fetch_names_set.end())
            {
                attributes_to_fetch_filter[i] = true;
            }
        }
    }

    DictionaryStorageFetchRequest() = default;

    /// Check requested attributes size
    size_t attributesSize() const
    {
        return attributes_to_fetch_types.size();
    }

    /// Check if attribute with attribute_name was requested to fetch
    bool containsAttribute(const String & attribute_name) const
    {
        return attributes_to_fetch_names_set.find(attribute_name) != attributes_to_fetch_names_set.end();
    }

    /// Check if attribute with attribute_index should be filled during fetch
    bool shouldFillResultColumnWithIndex(size_t attribute_index) const
    {
        return attributes_to_fetch_filter[attribute_index];
    }

    /// Create columns for each of dictionary attributes
    MutableColumns makeAttributesResultColumns() const
    {
        MutableColumns result;
        result.reserve(attributes_to_fetch_types.size());

        for (const auto & type : attributes_to_fetch_types)
            result.emplace_back(type->createColumn());

        return result;
    }

    /// Filter only requested colums
    Columns filterRequestedColumns(MutableColumns & fetched_mutable_columns) const
    {
        Columns result;
        result.reserve(attributes_to_fetch_types.size());

        for (size_t fetch_request_index = 0; fetch_request_index < attributes_to_fetch_types.size(); ++fetch_request_index)
            if (shouldFillResultColumnWithIndex(fetch_request_index))
                result.emplace_back(std::move(fetched_mutable_columns[fetch_request_index]));

        return result;
    }
private:
    std::unordered_set<String> attributes_to_fetch_names_set;
    std::vector<bool> attributes_to_fetch_filter;
    DataTypes attributes_to_fetch_types;
};

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

/// TODO: Remove DictionaryDefaultValueExtractor
class DefaultValueProvider final
{
public:
    explicit DefaultValueProvider(Field default_value_, ColumnPtr default_values_column_ = nullptr)
        : default_value(std::move(default_value_))
        , default_values_column(default_values_column_)
    {
    }

    Field getDefaultValue(size_t row) const
    {
        if (default_values_column)
            return (*default_values_column)[row];

        return default_value;
    }
private:
    Field default_value;
    ColumnPtr default_values_column;
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

template <DictionaryKeyType key_type>
class DictionaryKeysExtractor
{
public:
    using KeyType = std::conditional_t<key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(key_type != DictionaryKeyType::range, "Range key type is not supported by DictionaryKeysExtractor");

    explicit DictionaryKeysExtractor(const Columns & key_columns)
    {
        assert(!key_columns.empty());

        if constexpr (key_type == DictionaryKeyType::simple)
            keys = getColumnVectorData(key_columns.front());
        else
            keys = deserializeKeyColumnsInArena(key_columns, complex_keys_temporary_arena);
    }

    explicit DictionaryKeysExtractor(const Columns & key_columns, Arena & existing_arena)
    {
        assert(!key_columns.empty());

        if constexpr (key_type == DictionaryKeyType::simple)
            keys = getColumnVectorData(key_columns.front());
        else
            keys = deserializeKeyColumnsInArena(key_columns, existing_arena);
    }


    const PaddedPODArray<KeyType> & getKeys() const
    {
        return keys;
    }

private:
    static PaddedPODArray<UInt64> getColumnVectorData(const ColumnPtr column)
    {
        PaddedPODArray<UInt64> result;

        auto full_column = column->convertToFullColumnIfConst();
        const auto *vector_col = checkAndGetColumn<ColumnVector<UInt64>>(full_column.get());

        if (!vector_col)
            throw Exception{ErrorCodes::TYPE_MISMATCH, "Column type mismatch for simple key expected UInt64"};

        result.assign(vector_col->getData());

        return result;
    }

    static PaddedPODArray<StringRef> deserializeKeyColumnsInArena(const Columns & key_columns, Arena & temporary_arena)
    {
        size_t keys_size = key_columns.front()->size();

        PaddedPODArray<StringRef> result;
        result.reserve(keys_size);

        PaddedPODArray<StringRef> temporary_column_data(key_columns.size());

        for (size_t key_index = 0; key_index < keys_size; ++key_index)
        {
            size_t allocated_size_for_columns = 0;
            const char * block_start = nullptr;

            for (size_t column_index = 0; column_index < key_columns.size(); ++column_index)
            {
                const auto & column = key_columns[column_index];
                temporary_column_data[column_index] = column->serializeValueIntoArena(key_index, temporary_arena, block_start);
                allocated_size_for_columns += temporary_column_data[column_index].size;
            }

            result.push_back(StringRef{block_start, allocated_size_for_columns});
        }

        return result;
    }

    PaddedPODArray<KeyType> keys;
    Arena complex_keys_temporary_arena;
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
