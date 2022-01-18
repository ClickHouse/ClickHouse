#include "HashedDictionary.h"
#include <ext/size.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include "ClickHouseDictionarySource.h"
#include <Core/Defines.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesDecimal.h>

namespace
{

/// NOTE: Trailing return type is explicitly specified for SFINAE.

/// google::sparse_hash_map
template <typename T> auto first(const T & value) -> decltype(value.first) { return value.first; } // NOLINT
template <typename T> auto second(const T & value) -> decltype(value.second) { return value.second; } // NOLINT

/// HashMap
template <typename T> auto first(const T & value) -> decltype(value.getKey()) { return value.getKey(); } // NOLINT
template <typename T> auto second(const T & value) -> decltype(value.getMapped()) { return value.getMapped(); } // NOLINT

}

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}


HashedDictionary::HashedDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_,
    bool sparse_,
    BlockPtr saved_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , sparse(sparse_)
    , saved_block{std::move(saved_block_)}
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}


void HashedDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    DictionaryDefaultValueExtractor<UInt64> extractor(null_value);

    getItemsImpl<UInt64, UInt64>(
        *hierarchical_attribute,
        ids,
        [&](const size_t row, const UInt64 value) { out[row] = value; },
        extractor);
}


/// Allow to use single value in same way as array.
static inline HashedDictionary::Key getAt(const PaddedPODArray<HashedDictionary::Key> & arr, const size_t idx)
{
    return arr[idx];
}
static inline HashedDictionary::Key getAt(const HashedDictionary::Key & value, const size_t)
{
    return value;
}

template <typename AttrType, typename ChildType, typename AncestorType>
void HashedDictionary::isInAttrImpl(const AttrType & attr, const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    const auto rows = out.size();

    for (const auto row : ext::range(0, rows))
    {
        auto id = getAt(child_ids, row);
        const auto ancestor_id = getAt(ancestor_ids, row);

        for (size_t i = 0; id != null_value && id != ancestor_id && i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
        {
            auto it = attr.find(id);
            if (it != std::end(attr))
                id = second(*it);
            else
                break;
        }

        out[row] = id != null_value && id == ancestor_id;
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}
template <typename ChildType, typename AncestorType>
void HashedDictionary::isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    if (!sparse)
        return isInAttrImpl(*std::get<CollectionPtrType<Key>>(hierarchical_attribute->maps), child_ids, ancestor_ids, out);
    return isInAttrImpl(*std::get<SparseCollectionPtrType<Key>>(hierarchical_attribute->sparse_maps), child_ids, ancestor_ids, out);
}

void HashedDictionary::isInVectorVector(
    const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void HashedDictionary::isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void HashedDictionary::isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_id, ancestor_ids, out);
}

ColumnPtr HashedDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes &,
    const ColumnPtr default_values_column) const
{
    ColumnPtr result;

    PaddedPODArray<Key> backup_storage;
    const auto & ids = getColumnVectorData(this, key_columns.front(), backup_storage);

    auto size = ids.size();

    const auto & attribute = getAttribute(attribute_name);
    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        const auto attribute_null_value = std::get<ValueType>(attribute.null_values);
        AttributeType null_value = static_cast<AttributeType>(attribute_null_value);
        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(std::move(null_value), default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);

        if constexpr (std::is_same_v<AttributeType, String>)
        {
            auto * out = column.get();

            getItemsImpl<StringRef, StringRef>(
                attribute,
                ids,
                [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            getItemsImpl<AttributeType, AttributeType>(
                attribute,
                ids,
                [&](const size_t row, const auto value) { return out[row] = value; },
                default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    if (attribute.nullable_set)
    {
        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(size, false);
        ColumnUInt8::Container& vec_null_map_to = col_null_map_to->getData();

        for (size_t row = 0; row < ids.size(); ++row)
        {
            auto id = ids[row];

            if (attribute.nullable_set->find(id) != nullptr)
                vec_null_map_to[row] = true;
        }

        result = ColumnNullable::create(result, std::move(col_null_map_to));
    }

    return result;
}

ColumnUInt8::Ptr HashedDictionary::hasKeys(const Columns & key_columns, const DataTypes &) const
{
    PaddedPODArray<Key> backup_storage;
    const auto& ids = getColumnVectorData(this, key_columns.front(), backup_storage);

    size_t ids_count = ext::size(ids);

    auto result = ColumnUInt8::create(ext::size(ids));
    auto& out = result->getData();

    const auto & attribute = attributes.front();

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        has<AttributeType>(attribute, ids, out);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    query_count.fetch_add(ids_count, std::memory_order_relaxed);

    return result;
}

void HashedDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttribute(attribute, attribute.null_value));

        if (attribute.hierarchical)
        {
            hierarchical_attribute = &attributes.back();

            if (hierarchical_attribute->type != AttributeUnderlyingType::utUInt64)
                throw Exception{full_name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
        }
    }
}

void HashedDictionary::blockToAttributes(const Block & block)
{
    const auto & id_column = *block.safeGetByPosition(0).column;

    for (const size_t attribute_idx : ext::range(0, attributes.size()))
    {
        const IColumn & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;
        auto & attribute = attributes[attribute_idx];

        for (const auto row_idx : ext::range(0, id_column.size()))
            if (setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]))
                ++element_count;
    }
}

void HashedDictionary::updateData()
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
        auto stream = source_ptr->loadUpdatedAll();
        stream->readPrefix();

        while (Block block = stream->read())
        {
            const auto & saved_id_column = *saved_block->safeGetByPosition(0).column;
            const auto & update_id_column = *block.safeGetByPosition(0).column;

            std::unordered_map<Key, std::vector<size_t>> update_ids;
            for (size_t row = 0; row < update_id_column.size(); ++row)
            {
                const auto id = update_id_column.get64(row);
                update_ids[id].push_back(row);
            }

            const size_t saved_rows = saved_id_column.size();
            IColumn::Filter filter(saved_rows);
            std::unordered_map<Key, std::vector<size_t>>::iterator it;

            for (size_t row = 0; row < saved_id_column.size(); ++row)
            {
                auto id = saved_id_column.get64(row);
                it = update_ids.find(id);

                if (it != update_ids.end())
                    filter[row] = 0;
                else
                    filter[row] = 1;
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

template <typename T>
void HashedDictionary::resize(Attribute & attribute, size_t added_rows)
{
    if (!sparse)
    {
        const auto & map_ref = std::get<CollectionPtrType<T>>(attribute.maps);
        added_rows += map_ref->size();
        map_ref->reserve(added_rows);
    }
    else
    {
        const auto & map_ref = std::get<SparseCollectionPtrType<T>>(attribute.sparse_maps);
        added_rows += map_ref->size();
        map_ref->resize(added_rows);
    }
}

template <>
void HashedDictionary::resize<String>(Attribute & attribute, size_t added_rows)
{
    resize<StringRef>(attribute, added_rows);
}

void HashedDictionary::resize(size_t added_rows)
{
    if (!added_rows)
        return;

    for (auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            resize<AttributeType>(attribute, added_rows);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }
}

void HashedDictionary::loadData()
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
        throw Exception{full_name + ": dictionary source is empty and 'require_nonempty' property is set.", ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void HashedDictionary::addAttributeSize(const Attribute & attribute)
{
    if (!sparse)
    {
        const auto & map_ref = std::get<CollectionPtrType<T>>(attribute.maps);
        bytes_allocated += sizeof(CollectionType<T>) + map_ref->getBufferSizeInBytes();
        bucket_count = map_ref->getBufferSizeInCells();
    }
    else
    {
        const auto & map_ref = std::get<SparseCollectionPtrType<T>>(attribute.sparse_maps);
        bucket_count = map_ref->bucket_count();

        /** TODO: more accurate calculation */
        bytes_allocated += sizeof(SparseCollectionType<T>);
        bytes_allocated += bucket_count;
        bytes_allocated += map_ref->size() * (sizeof(Key) + sizeof(T));
    }
}

template <>
void HashedDictionary::addAttributeSize<String>(const Attribute & attribute)
{
    addAttributeSize<StringRef>(attribute);
    bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
}

void HashedDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            addAttributeSize<AttributeType>(attribute);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);

        bytes_allocated += sizeof(attribute.nullable_set);

        if (attribute.nullable_set.has_value())
            bytes_allocated = attribute.nullable_set->getBufferSizeInBytes();
    }

    if (saved_block)
        bytes_allocated += saved_block->allocatedBytes();
}

template <typename T>
void HashedDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
    if (!sparse)
        attribute.maps = std::make_unique<CollectionType<T>>();
    else
        attribute.sparse_maps = std::make_unique<SparseCollectionType<T>>();
}

template <>
void HashedDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    const String & string = null_value.get<String>();
    const char * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    attribute.null_values.emplace<StringRef>(string_in_arena, string.size());

    if (!sparse)
        attribute.maps = std::make_unique<CollectionType<StringRef>>();
    else
        attribute.sparse_maps = std::make_unique<SparseCollectionType<StringRef>>();
}

HashedDictionary::Attribute HashedDictionary::createAttribute(const DictionaryAttribute& attribute, const Field & null_value)
{
    auto nullable_set = attribute.is_nullable ? std::make_optional<NullableSet>() : std::optional<NullableSet>{};
    Attribute attr{attribute.underlying_type, std::move(nullable_set), {}, {}, {}, {}};

    auto type_call = [&, this](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        createAttributeImpl<AttributeType>(attr, null_value);
    };

    callOnDictionaryAttributeType(attribute.underlying_type, type_call);

    return attr;
}


template <typename AttributeType, typename OutputType, typename MapType, typename ValueSetter, typename DefaultValueExtractor>
void HashedDictionary::getItemsAttrImpl(
    const MapType & attr,
    const PaddedPODArray<Key> & ids,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto rows = ext::size(ids);

    for (const auto i : ext::range(0, rows))
    {
        const auto it = attr.find(ids[i]);
        set_value(i, it != attr.end() ? static_cast<OutputType>(second(*it)) : default_value_extractor[i]);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultValueExtractor>
void HashedDictionary::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    if (!sparse)
        return getItemsAttrImpl<AttributeType, OutputType>(*std::get<CollectionPtrType<AttributeType>>(attribute.maps), ids, set_value, default_value_extractor);
    return getItemsAttrImpl<AttributeType, OutputType>(*std::get<SparseCollectionPtrType<AttributeType>>(attribute.sparse_maps), ids, set_value, default_value_extractor);
}


template <typename T>
bool HashedDictionary::setAttributeValueImpl(Attribute & attribute, const Key id, const T value)
{
    if (!sparse)
    {
        auto & map = *std::get<CollectionPtrType<T>>(attribute.maps);
        return map.insert({id, value}).second;
    }
    else
    {
        auto & map = *std::get<SparseCollectionPtrType<T>>(attribute.sparse_maps);
        return map.insert({id, value}).second;
    }
}

template <>
bool HashedDictionary::setAttributeValueImpl<String>(Attribute & attribute, const Key id, const String value)
{
    const auto * string_in_arena = attribute.string_arena->insert(value.data(), value.size());
    return setAttributeValueImpl<StringRef>(attribute, id, StringRef{string_in_arena, value.size()});
}

bool HashedDictionary::setAttributeValue(Attribute & attribute, const Key id, const Field & value)
{
    bool result = false;

    auto type_call = [&, this](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        if (attribute.nullable_set)
        {
            if (value.isNull())
            {
                result = attribute.nullable_set->insert(id).second;
                return;
            }
            else
            {
                attribute.nullable_set->erase(id);
            }
        }

        result = setAttributeValueImpl<AttributeType>(attribute, id, value.get<NearestFieldType<AttributeType>>());
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    return result;
}

const HashedDictionary::Attribute & HashedDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

template <typename T>
void HashedDictionary::has(const Attribute & attribute, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto & attr = *std::get<CollectionPtrType<T>>(attribute.maps);
    const auto rows = ext::size(ids);

    for (const auto i : ext::range(0, rows))
    {
        out[i] = attr.find(ids[i]) != nullptr;

        if (attribute.nullable_set && !out[i])
            out[i] = attribute.nullable_set->find(ids[i]) != nullptr;
    }
}

template <>
void HashedDictionary::has<String>(const Attribute & attribute, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    has<StringRef>(attribute, ids, out);
}

template <typename T, typename AttrType>
PaddedPODArray<HashedDictionary::Key> HashedDictionary::getIdsAttrImpl(const AttrType & attr) const
{
    PaddedPODArray<Key> ids;
    ids.reserve(attr.size());
    for (const auto & value : attr)
        ids.push_back(first(value));

    return ids;
}
template <typename T>
PaddedPODArray<HashedDictionary::Key> HashedDictionary::getIds(const Attribute & attribute) const
{
    if (!sparse)
        return getIdsAttrImpl<T>(*std::get<CollectionPtrType<T>>(attribute.maps));
    return getIdsAttrImpl<T>(*std::get<SparseCollectionPtrType<T>>(attribute.sparse_maps));
}

template <>
PaddedPODArray<HashedDictionary::Key> HashedDictionary::getIds<String>(const Attribute & attribute) const
{
    return getIds<StringRef>(attribute);
}

PaddedPODArray<HashedDictionary::Key> HashedDictionary::getIds() const
{
    const auto & attribute = attributes.front();
    PaddedPODArray<HashedDictionary::Key> result;

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        /// TODO: Check if order is satisfied
        result = getIds<AttributeType>(attribute);

        if (attribute.nullable_set)
        {
            for (const auto& value: *attribute.nullable_set)
                result.push_back(value.getKey());
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    return result;
}

BlockInputStreamPtr HashedDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<Key>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getIds(), column_names);
}

void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             bool sparse) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'hashed'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<HashedDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty, sparse);
    };
    using namespace std::placeholders;
    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e){ return create_layout(a, b, c, d, std::move(e), /* sparse = */ true); }, false);
}

}
