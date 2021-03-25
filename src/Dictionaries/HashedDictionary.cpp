#include "HashedDictionary.h"

#include <ext/size.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>

#include <Dictionaries/DictionaryBlockInputStream.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/HierarchyDictionariesUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
HashedDictionary<dictionary_key_type, sparse>::HashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_,
    BlockPtr saved_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , saved_block(std::move(saved_block_))
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    Arena temporary_complex_key_arena;

    const DictionaryAttribute & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);
    DefaultValueProvider default_value_provider(dictionary_attribute.null_value, default_values_column);

    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, temporary_complex_key_arena);
    const auto & requested_keys = extractor.getKeys();

    auto result_column = dictionary_attribute.type->createColumn();
    result_column->reserve(requested_keys.size());

    size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute = attributes[attribute_index];

    Field row_value_to_insert;

    if (unlikely(attribute.is_complex_type))
    {
        auto & attribute_container = std::get<CollectionType<Field>>(attribute.container);

        for (size_t requested_key_index = 0; requested_key_index < requested_keys.size(); ++requested_key_index)
        {
            auto & requested_key = requested_keys[requested_key_index];
            auto it = attribute_container.find(requested_key);

            if (it != attribute_container.end())
                row_value_to_insert = it->second;
            else
                row_value_to_insert = default_value_provider.getDefaultValue(requested_key_index);

            result_column->insert(row_value_to_insert);
        }
    }
    else
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;
            using ColumnType = std::conditional_t<
                std::is_same_v<AttributeType, String>,
                ColumnString,
                std::conditional_t<IsDecimalNumber<AttributeType>, ColumnDecimal<ValueType>, ColumnVector<AttributeType>>>;

            auto & attribute_container = std::get<CollectionType<ValueType>>(attribute.container);
            ColumnType & result_column_typed = static_cast<ColumnType &>(*result_column);

            if constexpr (std::is_same_v<ColumnType, ColumnString>)
            {
                for (size_t requested_key_index = 0; requested_key_index < requested_keys.size(); ++requested_key_index)
                {
                    auto & requested_key = requested_keys[requested_key_index];
                    auto it = attribute_container.find(requested_key);

                    if (it != attribute_container.end())
                    {
                        auto item = it->second;
                        result_column->insertData(item.data, item.size);
                    }
                    else
                    {
                        row_value_to_insert = default_value_provider.getDefaultValue(requested_key_index);
                        result_column->insert(row_value_to_insert);
                    }
                }
            }
            else
            {
                auto & result_data = result_column_typed.getData();

                for (size_t requested_key_index = 0; requested_key_index < requested_keys.size(); ++requested_key_index)
                {
                    auto & requested_key = requested_keys[requested_key_index];
                    auto it = attribute_container.find(requested_key);

                    if (it != attribute_container.end())
                    {
                        auto item = it->second;
                        result_data.emplace_back(item);
                    }
                    else
                    {
                        row_value_to_insert = default_value_provider.getDefaultValue(requested_key_index);
                        result_data.emplace_back(row_value_to_insert.get<NearestFieldType<ValueType>>());
                    }
                }
            }
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }

    query_count.fetch_add(requested_keys.size(), std::memory_order_relaxed);

    return result_column;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse>::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    if (dictionary_key_type == DictionaryKeyType::complex)
        dict_struct.validateKeyTypes(key_types);

    Arena complex_keys_arena;
    DictionaryKeysExtractor<dictionary_key_type> extractor(key_columns, complex_keys_arena);

    const auto & keys = extractor.getKeys();
    size_t keys_size = keys.size();

    auto result = ColumnUInt8::create(keys_size);
    auto& out = result->getData();

    const auto & attribute = attributes.front();

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;

        const auto & attribute_map = std::get<CollectionType<ValueType>>(attribute.container);

        for (size_t requested_key_index = 0; requested_key_index < keys_size; ++requested_key_index)
        {
            const auto & requested_key = keys[requested_key_index];
            out[requested_key_index] = attribute_map.find(requested_key) != attribute_map.end();
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    query_count.fetch_add(keys_size, std::memory_order_relaxed);

    return result;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getHierarchy(ColumnPtr key_column, const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        const UInt64 null_value = dictionary_attribute.null_value.get<UInt64>();
        const CollectionType<UInt64> & parent_keys_map = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key)
        {
            return parent_keys_map.find(key) != parent_keys_map.end();
        };

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it == parent_keys_map.end())
                return result;

            result = it->second;

            return result;
        };

        auto dictionary_hierarchy_array = getKeysHierarchyArray(keys, null_value, is_key_valid_func, get_parent_func);

        query_count.fetch_add(keys.size(), std::memory_order_relaxed);

        return dictionary_hierarchy_array;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnUInt8::Ptr HashedDictionary<dictionary_key_type, sparse>::isInHierarchy(
    ColumnPtr key_column,
    ColumnPtr in_key_column,
    const DataTypePtr &) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup_storage;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup_storage);

        PaddedPODArray<UInt64> keys_in_backup_storage;
        const auto & keys_in = getColumnVectorData(this, in_key_column, keys_in_backup_storage);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        auto & dictionary_attribute = dict_struct.attributes[hierarchical_attribute_index];
        auto & hierarchical_attribute = attributes[hierarchical_attribute_index];

        const UInt64 null_value = dictionary_attribute.null_value.get<UInt64>();
        const CollectionType<UInt64> & parent_keys_map = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        auto is_key_valid_func = [&](auto & key)
        {
            return parent_keys_map.find(key) != parent_keys_map.end();
        };

        auto get_parent_func = [&](auto & hierarchy_key)
        {
            std::optional<UInt64> result;

            auto it = parent_keys_map.find(hierarchy_key);

            if (it == parent_keys_map.end())
                return result;

            result = it->second;

            return result;
        };

        auto is_in_hierarchy_result = isInKeysHierarchy(keys, keys_in, null_value, is_key_valid_func, get_parent_func);

        auto result = ColumnUInt8::create();
        result->getData() = std::move(is_in_hierarchy_result);

        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
ColumnPtr HashedDictionary<dictionary_key_type, sparse>::getDescendants(
    ColumnPtr key_column,
    const DataTypePtr &,
    size_t level) const
{
    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
    {
        PaddedPODArray<UInt64> keys_backup;
        const auto & keys = getColumnVectorData(this, key_column, keys_backup);

        size_t hierarchical_attribute_index = *dict_struct.hierarchical_attribute_index;

        const auto & hierarchical_attribute = attributes[hierarchical_attribute_index];
        const CollectionType<UInt64> & parent_keys = std::get<CollectionType<UInt64>>(hierarchical_attribute.container);

        HashMap<UInt64, PaddedPODArray<UInt64>> parent_to_child;

        for (const auto & [key, value] : parent_keys)
            parent_to_child[value].emplace_back(key);

        auto result = getDescendantsArray(keys, parent_to_child, level);
        return result;
    }
    else
        return nullptr;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & dictionary_attribute : dict_struct.attributes)
    {
        bool is_complex_type = dictionary_attribute.is_nullable || dictionary_attribute.is_array;

        auto type_call = [&, this](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            std::unique_ptr<Arena> string_arena = std::is_same_v<AttributeType, String> ? std::make_unique<Arena>() : nullptr;

            if (is_complex_type)
            {
                Attribute attribute{dictionary_attribute.underlying_type, is_complex_type, CollectionType<Field>(), std::move(string_arena)};
                attributes.emplace_back(std::move(attribute));
            }
            else
            {
                Attribute attribute{dictionary_attribute.underlying_type, is_complex_type, CollectionType<ValueType>(), std::move(string_arena)};
                attributes.emplace_back(std::move(attribute));
            }
        };

        callOnDictionaryAttributeType(dictionary_attribute.underlying_type, type_call);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::updateData()
{
    if (!saved_block || saved_block->rows() == 0)
    {
        auto stream = source_ptr->loadUpdatedAll();
        stream->readPrefix();

        while (const auto block = stream->read())
        {
            /// We are using this to keep saved data if input stream consists of multiple blocks
            if (!saved_block)
                saved_block = std::make_shared<DB::Block>(block.cloneEmpty());
            for (const auto attribute_idx : ext::range(0, attributes.size() + 1))
            {
                const IColumn & update_column = *block.getByPosition(attribute_idx).column.get();
                MutableColumnPtr saved_column = saved_block->getByPosition(attribute_idx).column->assumeMutable();
                saved_column->insertRangeFrom(update_column, 0, update_column.size());
            }
        }
        stream->readSuffix();
    }
    else
    {
        Arena temporary_complex_key_arena;

        size_t skip_keys_size_offset = dict_struct.getKeysSize();

        Columns saved_block_key_columns;
        saved_block_key_columns.reserve(skip_keys_size_offset);

        /// Split into keys columns and attribute columns
        for (size_t i = 0; i < skip_keys_size_offset; ++i)
            saved_block_key_columns.emplace_back(saved_block->safeGetByPosition(i).column);

        DictionaryKeysExtractor<dictionary_key_type> saved_keys_extractor(saved_block_key_columns, temporary_complex_key_arena);
        const auto & saved_keys_extracted_from_block = saved_keys_extractor.getKeys();

        auto stream = source_ptr->loadUpdatedAll();
        stream->readPrefix();

        while (Block block = stream->read())
        {
            /// TODO: Rewrite
            Columns block_key_columns;
            block_key_columns.reserve(skip_keys_size_offset);

            /// Split into keys columns and attribute columns
            for (size_t i = 0; i < skip_keys_size_offset; ++i)
                block_key_columns.emplace_back(block.safeGetByPosition(i).column);

            DictionaryKeysExtractor<dictionary_key_type> block_keys_extractor(saved_block_key_columns, temporary_complex_key_arena);
            const auto & keys_extracted_from_block = block_keys_extractor.getKeys();

            absl::flat_hash_map<KeyType, std::vector<size_t>, DefaultHash<KeyType>> update_keys;
            for (size_t row = 0; row < keys_extracted_from_block.size(); ++row)
            {
                const auto key = keys_extracted_from_block[row];
                update_keys[key].push_back(row);
            }

            IColumn::Filter filter(saved_keys_extracted_from_block.size());

            for (size_t row = 0; row < saved_keys_extracted_from_block.size(); ++row)
            {
                auto key = saved_keys_extracted_from_block[row];
                auto it = update_keys.find(key);
                filter[row] = (it == update_keys.end());
            }

            auto block_columns = block.mutateColumns();
            for (const auto attribute_idx : ext::range(0, attributes.size() + 1))
            {
                auto & column = saved_block->safeGetByPosition(attribute_idx).column;
                const auto & filtered_column = column->filter(filter, -1);
                block_columns[attribute_idx]->insertRangeFrom(*filtered_column.get(), 0, filtered_column->size());
            }

            saved_block->setColumns(std::move(block_columns));
        }

        stream->readSuffix();
    }

    if (saved_block)
    {
        resize(saved_block->rows());
        blockToAttributes(*saved_block.get());
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::blockToAttributes(const Block & block [[maybe_unused]])
{
    Arena temporary_complex_key_arena;

    size_t skip_keys_size_offset = dict_struct.getKeysSize();

    Columns key_columns;
    key_columns.reserve(skip_keys_size_offset);

    /// Split into keys columns and attribute columns
    for (size_t i = 0; i < skip_keys_size_offset; ++i)
        key_columns.emplace_back(block.safeGetByPosition(i).column);

    DictionaryKeysExtractor<dictionary_key_type> keys_extractor(key_columns, temporary_complex_key_arena);
    const auto & keys_extracted_from_block = keys_extractor.getKeys();

    Field column_value_to_insert;

    for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
    {
        const IColumn & attribute_column = *block.safeGetByPosition(skip_keys_size_offset + attribute_index).column;
        auto & attribute = attributes[attribute_index];

        getAttributeContainer(attribute_index, [&](auto & container)
        {
            using ContainerType = std::decay_t<decltype(container)>;
            using AttributeValueType = typename ContainerType::mapped_type;

            for (size_t key_index = 0; key_index < keys_extracted_from_block.size(); ++key_index)
            {
                auto key = keys_extracted_from_block[key_index];
                auto it = container.find(key);

                if (it != container.end())
                    continue;

                if constexpr (std::is_same_v<KeyType, StringRef>)
                    key = copyKeyInArena(key);

                attribute_column.get(key_index, column_value_to_insert);

                if constexpr (std::is_same_v<AttributeValueType, Field>)
                {
                    container.insert({key, column_value_to_insert});
                }
                else if constexpr (std::is_same_v<AttributeValueType, StringRef>)
                {
                    String & value_to_insert = column_value_to_insert.get<String>();
                    size_t value_to_insert_size = value_to_insert.size();

                    const char * string_in_arena = attribute.string_arena->insert(value_to_insert.data(), value_to_insert_size);

                    StringRef string_in_arena_reference = StringRef{string_in_arena, value_to_insert_size};
                    container.insert({key, string_in_arena_reference});
                }
                else
                {
                    auto value_to_insert = column_value_to_insert.get<NearestFieldType<AttributeValueType>>();
                    container.insert({key, value_to_insert});
                }

                ++element_count;
            }
        });
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::resize(size_t added_rows)
{
    if (unlikely(!added_rows))
        return;

    for (size_t attribute_index = 0; attribute_index < attributes.size(); ++attribute_index)
    {
        getAttributeContainer(attribute_index, [added_rows](auto & attribute_map)
        {
            size_t reserve_size = added_rows + attribute_map.size();

            if constexpr (sparse)
                attribute_map.resize(reserve_size);
            else
                attribute_map.reserve(reserve_size);
        });
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
StringRef HashedDictionary<dictionary_key_type, sparse>::copyKeyInArena(StringRef key)
{
    size_t key_size = key.size;
    char * place_for_key = complex_key_arena.alloc(key_size);
    memcpy(reinterpret_cast<void *>(place_for_key), reinterpret_cast<const void *>(key.data), key_size);
    StringRef updated_key{place_for_key, key_size};
    return updated_key;
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        auto stream = source_ptr->loadAll();

        stream->readPrefix();

        while (const auto block = stream->read())
        {
            resize(block.rows());
            blockToAttributes(block);
        }

        stream->readSuffix();
    }
    else
        updateData();

    if (require_nonempty && 0 == element_count)
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY,
            "({}): dictionary source is empty and 'require_nonempty' property is set.",
            full_name);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
void HashedDictionary<dictionary_key_type, sparse>::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (size_t i = 0; i < attributes.size(); ++i)
    {
        getAttributeContainer(i, [&](const auto & container)
        {
            /// TODO: Calculate
            bytes_allocated += sizeof(container);
        });
    }

    bytes_allocated += complex_key_arena.size();
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
BlockInputStreamPtr HashedDictionary<dictionary_key_type, sparse>::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    PaddedPODArray<HashedDictionary::KeyType> keys;

    if (!attributes.empty())
        getAttributeContainer(0, [&](auto & container)
        {
            keys.reserve(container.size());

            for (const auto & [key, value] : container)
            {
                (void)(value);
                keys.emplace_back(key);
            }
        });

    if constexpr (dictionary_key_type == DictionaryKeyType::simple)
        return std::make_shared<DictionaryBlockInputStream>(shared_from_this(), max_block_size, std::move(keys), column_names);
    else
        return std::make_shared<DictionaryBlockInputStream>(shared_from_this(), max_block_size, keys, column_names);
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
template <typename GetContainerFunc>
void HashedDictionary<dictionary_key_type, sparse>::getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func)
{
    assert(attribute_index < attributes.size());

    auto & attribute = attributes[attribute_index];

    if (unlikely(attribute.is_complex_type))
    {
        auto & attribute_container = std::get<CollectionType<Field>>(attribute.container);
        std::forward<GetContainerFunc>(get_container_func)(attribute_container);
    }
    else
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;

            auto & attribute_container = std::get<CollectionType<ValueType>>(attribute.container);
            std::forward<GetContainerFunc>(get_container_func)(attribute_container);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }
}

template <DictionaryKeyType dictionary_key_type, bool sparse>
template <typename GetContainerFunc>
void HashedDictionary<dictionary_key_type, sparse>::getAttributeContainer(size_t attribute_index, GetContainerFunc && get_container_func) const
{
    const_cast<std::decay_t<decltype(*this)> *>(this)->getAttributeContainer(attribute_index, [&](auto & attribute_container)
    {
        std::forward<GetContainerFunc>(get_container_func)(attribute_container);
    });
}

template class HashedDictionary<DictionaryKeyType::simple, true>;
template class HashedDictionary<DictionaryKeyType::simple, false>;
template class HashedDictionary<DictionaryKeyType::complex, true>;
template class HashedDictionary<DictionaryKeyType::complex, false>;

void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             DictionaryKeyType dictionary_key_type,
                             bool sparse) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        else if (dictionary_key_type == DictionaryKeyType::complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        if (dictionary_key_type == DictionaryKeyType::simple)
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::simple, true>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::simple, false>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
        }
        else
        {
            if (sparse)
                return std::make_unique<HashedDictionary<DictionaryKeyType::complex, true>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
            else
                return std::make_unique<HashedDictionary<DictionaryKeyType::complex, false>>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
        }
    };

    using namespace std::placeholders;

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::simple, /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::simple, /* sparse = */ true); }, false);
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::complex, /* sparse = */ true); }, true);
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), DictionaryKeyType::complex, /* sparse = */ true); }, true);

}

}
