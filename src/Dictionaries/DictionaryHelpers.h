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
    extern const int BAD_ARGUMENTS;
}

/** Simple helper for getting default.
  * Initialized with default value and default values column.
  * If default values column is not null default value is taken from column.
  * If default value is null default value is taken from initializer.
  */
class DefaultValueProvider final
{
public:
    explicit DefaultValueProvider(Field default_value_, ColumnPtr default_values_column_ = nullptr)
        : default_value(std::move(default_value_))
        , default_values_column(default_values_column_)
    {
    }

    inline bool isConstant() const { return default_values_column == nullptr; }

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
    DictionaryStorageFetchRequest(const DictionaryStructure & structure, const Strings & attributes_names_to_fetch, Columns attributes_default_values_columns)
        : attributes_to_fetch_names_set(attributes_names_to_fetch.begin(), attributes_names_to_fetch.end())
        , attributes_to_fetch_filter(structure.attributes.size(), false)
    {
        assert(attributes_default_values_columns.size() == attributes_names_to_fetch.size());

        if (attributes_to_fetch_names_set.size() != attributes_names_to_fetch.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Attribute names to fetch should be unique");

        size_t attributes_size = structure.attributes.size();
        dictionary_attributes_types.reserve(attributes_size);
        attributes_default_value_providers.reserve(attributes_to_fetch_names_set.size());

        size_t default_values_column_index = 0;
        for (size_t i = 0; i < attributes_size; ++i)
        {
            const auto & dictionary_attribute = structure.attributes[i];
            const auto & name = dictionary_attribute.name;
            const auto & type = dictionary_attribute.type;
            dictionary_attributes_types.emplace_back(type);

            if (attributes_to_fetch_names_set.find(name) != attributes_to_fetch_names_set.end())
            {
                attributes_to_fetch_filter[i] = true;
                attributes_default_value_providers.emplace_back(dictionary_attribute.null_value, attributes_default_values_columns[default_values_column_index]);
                ++default_values_column_index;
            }
            else
                attributes_default_value_providers.emplace_back(dictionary_attribute.null_value);
        }
    }

    DictionaryStorageFetchRequest() = default;

    /// Check requested attributes size
    ALWAYS_INLINE size_t attributesSize() const
    {
        return dictionary_attributes_types.size();
    }

    /// Check if attribute with attribute_name was requested to fetch
    ALWAYS_INLINE bool containsAttribute(const String & attribute_name) const
    {
        return attributes_to_fetch_names_set.find(attribute_name) != attributes_to_fetch_names_set.end();
    }

    /// Check if attribute with attribute_index should be filled during fetch
    ALWAYS_INLINE bool shouldFillResultColumnWithIndex(size_t attribute_index) const
    {
        return attributes_to_fetch_filter[attribute_index];
    }

    const DataTypePtr & dataTypeAtIndex(size_t attribute_index) const
    {
        return dictionary_attributes_types[attribute_index];
    }

    const DefaultValueProvider & defaultValueProviderAtIndex(size_t attribute_index) const
    {
        return attributes_default_value_providers[attribute_index];
    }

    /// Create columns for each of dictionary attributes
    MutableColumns makeAttributesResultColumns() const
    {
        MutableColumns result;
        result.reserve(dictionary_attributes_types.size());

        for (const auto & type : dictionary_attributes_types)
            result.emplace_back(type->createColumn());

        return result;
    }

    Columns makeAttributesResultColumnsNonMutable() const
    {
        Columns result;
        result.reserve(dictionary_attributes_types.size());

        for (const auto & type : dictionary_attributes_types)
            result.emplace_back(type->createColumn());

        return result;
    }

    /// Filter only requested columns
    Columns filterRequestedColumns(MutableColumns & fetched_mutable_columns) const
    {
        Columns result;
        result.reserve(dictionary_attributes_types.size());

        for (size_t fetch_request_index = 0; fetch_request_index < dictionary_attributes_types.size(); ++fetch_request_index)
            if (shouldFillResultColumnWithIndex(fetch_request_index))
                result.emplace_back(std::move(fetched_mutable_columns[fetch_request_index]));

        return result;
    }
private:
    std::unordered_set<String> attributes_to_fetch_names_set;
    std::vector<bool> attributes_to_fetch_filter;
    std::vector<DefaultValueProvider> attributes_default_value_providers;
    DataTypes dictionary_attributes_types;
};

static inline void insertDefaultValuesIntoColumns(
    MutableColumns & columns,
    const DictionaryStorageFetchRequest & fetch_request,
    size_t row_index)
{
    size_t columns_size = columns.size();

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        const auto & column = columns[column_index];
        const auto & default_value_provider = fetch_request.defaultValueProviderAtIndex(column_index);

        if (fetch_request.shouldFillResultColumnWithIndex(column_index))
            column->insert(default_value_provider.getDefaultValue(row_index));
    }
}

/// Deserialize column value and insert it in columns.
/// Skip unnecessary columns that were not requested from deserialization.
static inline void deserializeAndInsertIntoColumns(
    MutableColumns & columns,
    const DictionaryStorageFetchRequest & fetch_request,
    const char * place_for_serialized_columns)
{
    size_t columns_size = columns.size();

    for (size_t column_index = 0; column_index < columns_size; ++column_index)
    {
        const auto & column = columns[column_index];

        if (fetch_request.shouldFillResultColumnWithIndex(column_index))
            place_for_serialized_columns = column->deserializeAndInsertFromArena(place_for_serialized_columns);
        else
            place_for_serialized_columns = column->skipSerializedInArena(place_for_serialized_columns);
    }
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

    explicit DictionaryDefaultValueExtractor(DictionaryAttributeType attribute_default_value, ColumnPtr default_values_column_ = nullptr)
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
