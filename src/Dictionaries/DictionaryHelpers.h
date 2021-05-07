#pragma once

#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Core/Block.h>
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
    DictionaryStorageFetchRequest(
        const DictionaryStructure & structure,
        const Strings & attributes_names_to_fetch,
        DataTypes attributes_to_fetch_result_types,
        Columns attributes_default_values_columns)
        : attributes_to_fetch_names_set(attributes_names_to_fetch.begin(), attributes_names_to_fetch.end())
        , attributes_to_fetch_filter(structure.attributes.size(), false)
    {
        assert(attributes_default_values_columns.size() == attributes_names_to_fetch.size());

        if (attributes_to_fetch_names_set.size() != attributes_names_to_fetch.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Attribute names to fetch should be unique");

        size_t attributes_size = structure.attributes.size();
        dictionary_attributes_types.reserve(attributes_size);
        attributes_default_value_providers.reserve(attributes_to_fetch_names_set.size());

        size_t attributes_to_fetch_index = 0;
        for (size_t i = 0; i < attributes_size; ++i)
        {
            const auto & dictionary_attribute = structure.attributes[i];
            const auto & name = dictionary_attribute.name;
            const auto & type = dictionary_attribute.type;
            dictionary_attributes_types.emplace_back(type);

            if (attributes_to_fetch_names_set.find(name) != attributes_to_fetch_names_set.end())
            {
                attributes_to_fetch_filter[i] = true;
                auto & attribute_to_fetch_result_type = attributes_to_fetch_result_types[attributes_to_fetch_index];

                if (!attribute_to_fetch_result_type->equals(*type))
                    throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Attribute type does not match, expected ({}), found ({})",
                    attribute_to_fetch_result_type->getName(),
                    type->getName());

                attributes_default_value_providers.emplace_back(dictionary_attribute.null_value, attributes_default_values_columns[attributes_to_fetch_index]);
                ++attributes_to_fetch_index;
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
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Unsupported attribute type.");
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
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type of default column is not the same as dictionary attribute type.");
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
class DictionaryKeysArenaHolder;

template <>
class DictionaryKeysArenaHolder<DictionaryKeyType::simple>
{
public:
    static Arena * getComplexKeyArena() { return nullptr; }
};

template <>
class DictionaryKeysArenaHolder<DictionaryKeyType::complex>
{
public:

    Arena * getComplexKeyArena() { return &complex_key_arena; }

private:
    Arena complex_key_arena;
};


template <DictionaryKeyType key_type>
class DictionaryKeysExtractor
{
public:
    using KeyType = std::conditional_t<key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(key_type != DictionaryKeyType::range, "Range key type is not supported by DictionaryKeysExtractor");

    explicit DictionaryKeysExtractor(const Columns & key_columns_, Arena * complex_key_arena_)
        : key_columns(key_columns_)
        , complex_key_arena(complex_key_arena_)
    {
        assert(!key_columns.empty());

        if constexpr (key_type == DictionaryKeyType::simple)
        {
            key_columns[0] = key_columns[0]->convertToFullColumnIfConst();

            const auto * vector_col = checkAndGetColumn<ColumnVector<UInt64>>(key_columns[0].get());
            if (!vector_col)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Column type mismatch for simple key expected UInt64");
        }

        keys_size = key_columns.front()->size();
    }

    inline size_t getKeysSize() const
    {
        return keys_size;
    }

    inline size_t getCurrentKeyIndex() const
    {
        return current_key_index;
    }

    inline KeyType extractCurrentKey()
    {
        assert(current_key_index < keys_size);

        if constexpr (key_type == DictionaryKeyType::simple)
        {
            const auto & column_vector = static_cast<const ColumnVector<UInt64> &>(*key_columns[0]);
            const auto & data = column_vector.getData();

            auto key = data[current_key_index];
            ++current_key_index;
            return key;
        }
        else
        {
            size_t allocated_size_for_columns = 0;
            const char * block_start = nullptr;

            for (const auto & column : key_columns)
            {
                StringRef serialized_data = column->serializeValueIntoArena(current_key_index, *complex_key_arena, block_start);
                allocated_size_for_columns += serialized_data.size;
            }

            ++current_key_index;
            current_complex_key = StringRef{block_start, allocated_size_for_columns};
            return  current_complex_key;
        }
    }

    void rollbackCurrentKey() const
    {
        if constexpr (key_type == DictionaryKeyType::complex)
            complex_key_arena->rollback(current_complex_key.size);
    }

    PaddedPODArray<KeyType> extractAllKeys()
    {
        PaddedPODArray<KeyType> result;
        result.reserve(keys_size - current_key_index);

        for (; current_key_index < keys_size;)
        {
            auto value = extractCurrentKey();
            result.emplace_back(value);
        }

        return result;
    }

    void reset()
    {
        current_key_index = 0;
    }
private:
    Columns key_columns;

    size_t keys_size = 0;
    size_t current_key_index = 0;

    KeyType current_complex_key {};
    Arena * complex_key_arena;
};

/** Merge block with blocks from stream. If there are duplicate keys in block they are filtered out.
  * In result block_to_update will be merged with blocks from stream.
  * Note: readPrefix readImpl readSuffix will be called on stream object during function execution.
  */
template <DictionaryKeyType dictionary_key_type>
void mergeBlockWithStream(
    size_t key_columns_size,
    Block & block_to_update,
    BlockInputStreamPtr & stream)
{
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by updatePreviousyLoadedBlockWithStream");

    Columns saved_block_key_columns;
    saved_block_key_columns.reserve(key_columns_size);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < key_columns_size; ++i)
        saved_block_key_columns.emplace_back(block_to_update.safeGetByPosition(i).column);

    DictionaryKeysArenaHolder<dictionary_key_type> arena_holder;
    DictionaryKeysExtractor<dictionary_key_type> saved_keys_extractor(saved_block_key_columns, arena_holder.getComplexKeyArena());
    auto saved_keys_extracted_from_block = saved_keys_extractor.extractAllKeys();

    /**
     * We create filter with our block to update size, because we want to filter out values that have duplicate keys
     * if they appear in blocks that we fetch from stream.
     * But first we try to filter out duplicate keys from existing block.
     * For example if we have block with keys 1, 2, 2, 2, 3, 3
     * Our filter will have [1, 0, 0, 1, 0, 1] after first stage.
     * We also update saved_key_to_index hash map for keys to point to their latest indexes.
     * For example if in blocks from stream we will get keys [4, 2, 3]
     * Our filter will be [1, 0, 0, 0, 0, 0].
     * After reading all blocks from stream we filter our duplicate keys from block_to_update and insert loaded columns.
     */

    IColumn::Filter filter(saved_keys_extracted_from_block.size(), true);

    HashMap<KeyType, size_t> saved_key_to_index;
    saved_key_to_index.reserve(saved_keys_extracted_from_block.size());

    size_t indexes_to_remove_count = 0;

    for (size_t i = 0; i < saved_keys_extracted_from_block.size(); ++i)
    {
        auto saved_key = saved_keys_extracted_from_block[i];
        auto [it, was_inserted] = saved_key_to_index.insert(makePairNoInit(saved_key, i));

        if (!was_inserted)
        {
            size_t index_to_remove = it->getMapped();
            filter[index_to_remove] = false;
            it->getMapped() = i;
            ++indexes_to_remove_count;
        }
    }

    auto result_fetched_columns = block_to_update.cloneEmptyColumns();

    stream->readPrefix();

    while (Block block = stream->read())
    {
        Columns block_key_columns;
        block_key_columns.reserve(key_columns_size);

        /// Split into keys columns and attribute columns
        for (size_t i = 0; i < key_columns_size; ++i)
            block_key_columns.emplace_back(block.safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> update_keys_extractor(block_key_columns, arena_holder.getComplexKeyArena());
        PaddedPODArray<KeyType> update_keys = update_keys_extractor.extractAllKeys();

        for (auto update_key : update_keys)
        {
            const auto * it = saved_key_to_index.find(update_key);
            if (it != nullptr)
            {
                size_t index_to_filter = it->getMapped();
                filter[index_to_filter] = false;
                ++indexes_to_remove_count;
            }
        }

        size_t rows = block.rows();

        for (size_t column_index = 0; column_index < block.columns(); ++column_index)
        {
            const auto update_column = block.safeGetByPosition(column_index).column;
            MutableColumnPtr & result_fetched_column = result_fetched_columns[column_index];

            result_fetched_column->insertRangeFrom(*update_column, 0, rows);
        }
    }

    stream->readSuffix();

    size_t result_fetched_rows = result_fetched_columns.front()->size();
    size_t filter_hint = filter.size() - indexes_to_remove_count;

    for (size_t column_index = 0; column_index < block_to_update.columns(); ++column_index)
    {
        auto & column = block_to_update.getByPosition(column_index).column;
        column = column->filter(filter, filter_hint);

        MutableColumnPtr mutable_column = column->assumeMutable();
        const IColumn & fetched_column = *result_fetched_columns[column_index];
        mutable_column->insertRangeFrom(fetched_column, 0, result_fetched_rows);
    }
}

/**
 * Returns ColumnVector data as PaddedPodArray.

 * If column is constant parameter backup_storage is used to store values.
 */
/// TODO: Remove
template <typename T>
static const PaddedPODArray<T> & getColumnVectorData(
    const IDictionary * dictionary,
    const ColumnPtr column,
    PaddedPODArray<T> & backup_storage)
{
    bool is_const_column = isColumnConst(*column);
    auto full_column = column->convertToFullColumnIfConst();
    auto vector_col = checkAndGetColumn<ColumnVector<T>>(full_column.get());

    if (!vector_col)
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "{}: type mismatch: column has wrong type expected {}",
            dictionary->getDictionaryID().getNameForLogs(),
            TypeName<T>::get());
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
