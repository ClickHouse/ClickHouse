#include "FlatDictionary.h"
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include <Core/Defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int UNSUPPORTED_METHOD;
}

static const auto initial_array_size = 1024;
static const auto max_array_size = 500000;


FlatDictionary::FlatDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_,
    BlockPtr saved_block_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , loaded_ids(initial_array_size, false)
    , saved_block{std::move(saved_block_)}
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}


void FlatDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    getItemsImpl<UInt64, UInt64>(
        *hierarchical_attribute,
        ids,
        [&](const size_t row, const UInt64 value) { out[row] = value; },
        [&](const size_t) { return null_value; });
}


/// Allow to use single value in same way as array.
static inline FlatDictionary::Key getAt(const PaddedPODArray<FlatDictionary::Key> & arr, const size_t idx)
{
    return arr[idx];
}
static inline FlatDictionary::Key getAt(const FlatDictionary::Key & value, const size_t)
{
    return value;
}

template <typename ChildType, typename AncestorType>
void FlatDictionary::isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    const auto & attr = std::get<ContainerType<Key>>(hierarchical_attribute->arrays);
    const auto rows = out.size();

    size_t loaded_size = attr.size();
    for (const auto row : ext::range(0, rows))
    {
        auto id = getAt(child_ids, row);
        const auto ancestor_id = getAt(ancestor_ids, row);

        for (size_t i = 0; id < loaded_size && id != null_value && id != ancestor_id && i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
            id = attr[id];

        out[row] = id != null_value && id == ancestor_id;
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


void FlatDictionary::isInVectorVector(
    const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void FlatDictionary::isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void FlatDictionary::isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_id, ancestor_ids, out);
}

ColumnPtr FlatDictionary::getColumn(
        const std::string& attribute_name,
        const DataTypePtr &,
        const Columns & key_columns,
        const DataTypes &,
        const ColumnPtr default_untyped) const
{
    ColumnPtr result;

    PaddedPODArray<Key> backup_storage;
    const auto & ids = getColumnDataAsPaddedPODArray(this, key_columns.front(), backup_storage);

    auto size = ids.size();

    const auto & attribute = getAttribute(attribute_name);

    ColumnUInt8::MutablePtr col_null_map_to;
    ColumnUInt8::Container * vec_null_map_to = nullptr;
    if (attribute.is_nullable)
    {
        col_null_map_to = ColumnUInt8::create(size, false);
        vec_null_map_to = &col_null_map_to->getData();
    }

    /// TODO: Check that attribute type is same as result type
    /// TODO: Check if const will work as expected

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        if constexpr (std::is_same_v<AttributeType, String>)
        {
            auto column_string = ColumnString::create();
            auto * out = column_string.get();

            if (default_untyped != nullptr)
            {
                if (const auto * const default_col = checkAndGetColumn<ColumnString>(*default_untyped))
                {
                    getItemsImpl<StringRef, StringRef>(
                        attribute,
                        ids,
                        [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                        [&](const size_t row) { return default_col->getDataAt(row); });
                }
                else if (const auto * const default_col_const = checkAndGetColumnConst<ColumnString>(default_untyped.get()))
                {
                    const auto & def = default_col_const->template getValue<String>();

                    getItemsImpl<StringRef, StringRef>(
                        attribute,
                        ids,
                        [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                        [&](const size_t) { return def; });
                }
            }
            else
            {
                const auto & null_value = std::get<StringRef>(attribute.null_values);

                getItemsImpl<StringRef, StringRef>(
                    attribute,
                    ids,
                    [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                    [&](const size_t) { return null_value; });
            }

            result = std::move(column_string);
        }
        else
        {
            using ResultColumnType
                = std::conditional_t<IsDecimalNumber<AttributeType>, ColumnDecimal<AttributeType>, ColumnVector<AttributeType>>;
            using ResultColumnPtr = typename ResultColumnType::MutablePtr;

            ResultColumnPtr column;

            if constexpr (IsDecimalNumber<AttributeType>)
            {
                // auto scale = getDecimalScale(*attribute.type);
                column = ColumnDecimal<AttributeType>::create(size, 0);
            }
            else if constexpr (IsNumber<AttributeType>)
                column = ColumnVector<AttributeType>::create(size);

            auto & out = column->getData();

            if (default_untyped != nullptr)
            {
                if (const auto * const default_col = checkAndGetColumn<ResultColumnType>(*default_untyped))
                {
                    getItemsImpl<AttributeType, AttributeType>(
                        attribute,
                        ids,
                        [&](const size_t row, const auto value) { return out[row] = value; },
                        [&](const size_t row) { return default_col->getData()[row]; }
                    );
                }
                else if (const auto * const default_col_const = checkAndGetColumnConst<ResultColumnType>(default_untyped.get()))
                {
                    const auto & def = default_col_const->template getValue<AttributeType>();

                    getItemsImpl<AttributeType, AttributeType>(
                        attribute,
                        ids,
                        [&](const size_t row, const auto value) { return out[row] = value; },
                        [&](const size_t) { return def; }
                    );
                }
            }
            else
            {
                const auto null_value = std::get<AttributeType>(attribute.null_values);

                getItemsImpl<AttributeType, AttributeType>(
                    attribute,
                    ids,
                    [&](const size_t row, const auto value) { return out[row] = value; },
                    [&](const size_t) { return null_value; }
                );
            }

            result = std::move(column);
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    /// TODO: Fix
    if (attribute.is_nullable)
    {
        for (size_t row = 0; row < ids.size(); ++row)
        {
            auto id = ids[row];
            if (attribute.nullable_set->find(id) != attribute.nullable_set->end())
            {
                (*vec_null_map_to)[row] = true;
            }
        }

        result = ColumnNullable::create(result, std::move(col_null_map_to));
    }

    return result;
}


ColumnUInt8::Ptr FlatDictionary::has(const Columns & key_columns, const DataTypes &) const
{
    PaddedPODArray<Key> backup_storage;
    const auto& ids = getColumnDataAsPaddedPODArray(this, key_columns.front(), backup_storage);

    auto result = ColumnUInt8::create(ext::size(ids));
    auto& out = result->getData();

    const auto ids_count = ext::size(ids);

    for (const auto i : ext::range(0, ids_count))
    {
        const auto id = ids[i];
        out[i] = id < loaded_ids.size() && loaded_ids[id];
    }

    query_count.fetch_add(ids_count, std::memory_order_relaxed);

    return result;
}

void FlatDictionary::createAttributes()
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

void FlatDictionary::blockToAttributes(const Block & block)
{
    const IColumn & id_column = *block.safeGetByPosition(0).column;
    element_count += id_column.size();

    for (const size_t attribute_idx : ext::range(0, attributes.size()))
    {
        const IColumn & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;
        Attribute & attribute = attributes[attribute_idx];

        for (const auto row_idx : ext::range(0, id_column.size()))
            setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
    }
}

void FlatDictionary::updateData()
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
        blockToAttributes(*saved_block.get());
}

void FlatDictionary::loadData()
{
    if (!source_ptr->hasUpdateField())
    {
        auto stream = source_ptr->loadAll();
        stream->readPrefix();

        while (const auto block = stream->read())
            blockToAttributes(block);

        stream->readSuffix();
    }
    else
        updateData();

    if (require_nonempty && 0 == element_count)
        throw Exception{full_name + ": dictionary source is empty and 'require_nonempty' property is set.", ErrorCodes::DICTIONARY_IS_EMPTY};
}


template <typename T>
void FlatDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & array_ref = std::get<ContainerType<T>>(attribute.arrays);
    bytes_allocated += sizeof(PaddedPODArray<T>) + array_ref.allocated_bytes();
    bucket_count = array_ref.capacity();
}

template <>
void FlatDictionary::addAttributeSize<String>(const Attribute & attribute)
{
    const auto & array_ref = std::get<ContainerType<StringRef>>(attribute.arrays);
    bytes_allocated += sizeof(PaddedPODArray<StringRef>) + array_ref.allocated_bytes();
    bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
    bucket_count = array_ref.capacity();
}

void FlatDictionary::calculateBytesAllocated()
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
    }
}


template <typename T>
void FlatDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
    const auto & null_value_ref = std::get<T>(attribute.null_values);
    attribute.arrays.emplace<ContainerType<T>>(initial_array_size, null_value_ref);
}

template <>
void FlatDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    const String & string = null_value.get<String>();
    const char * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    attribute.null_values.emplace<StringRef>(string_in_arena, string.size());
    attribute.arrays.emplace<ContainerType<StringRef>>(initial_array_size, StringRef(string_in_arena, string.size()));
}


FlatDictionary::Attribute FlatDictionary::createAttribute(const DictionaryAttribute& attribute, const Field & null_value)
{
    auto nullable_set = attribute.is_nullable ? std::make_unique<NullableSet>() : nullptr;
    Attribute attr{attribute.underlying_type, attribute.is_nullable, std::move(nullable_set), {}, {}, {}};

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        createAttributeImpl<AttributeType>(attr, null_value);
    };

    callOnDictionaryAttributeType(attribute.underlying_type, type_call);

    return attr;
}


template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void FlatDictionary::getItemsImpl(
    const Attribute & attribute, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto & attr = std::get<ContainerType<AttributeType>>(attribute.arrays);
    const auto rows = ext::size(ids);

    for (const auto row : ext::range(0, rows))
    {
        const auto id = ids[row];
        set_value(row, id < ext::size(attr) && loaded_ids[id] ? static_cast<OutputType>(attr[id]) : get_default(row));
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename T>
void FlatDictionary::resize(Attribute & attribute, const Key id)
{
    if (id >= max_array_size)
        throw Exception{full_name + ": identifier should be less than " + toString(max_array_size), ErrorCodes::ARGUMENT_OUT_OF_BOUND};

    auto & array = std::get<ContainerType<T>>(attribute.arrays);
    if (id >= array.size())
    {
        const size_t elements_count = id + 1; //id=0 -> elements_count=1
        loaded_ids.resize(elements_count, false);
        array.resize_fill(elements_count, std::get<T>(attribute.null_values));
    }
}

template <typename T>
void FlatDictionary::setAttributeValueImpl(Attribute & attribute, const Key id, const T & value)
{
    resize<T>(attribute, id);
    auto & array = std::get<ContainerType<T>>(attribute.arrays);
    array[id] = value;
    loaded_ids[id] = true;
}

template <>
void FlatDictionary::setAttributeValueImpl<String>(Attribute & attribute, const Key id, const String & value)
{
    resize<StringRef>(attribute, id);
    const auto * string_in_arena = attribute.string_arena->insert(value.data(), value.size());
    auto & array = std::get<ContainerType<StringRef>>(attribute.arrays);
    array[id] = StringRef{string_in_arena, value.size()};
    loaded_ids[id] = true;
}

void FlatDictionary::setAttributeValue(Attribute & attribute, const Key id, const Field & value)
{
    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        if (attribute.is_nullable)
        {
            if (value.isNull())
            {
                attribute.nullable_set->insert(id);
                loaded_ids[id] = true;
                return;
            }
            else
            {
                auto find_iter = attribute.nullable_set->find(id);
                if (find_iter != attribute.nullable_set->end())
                {
                    attribute.nullable_set->erase(find_iter);
                }
            }
        }

        setAttributeValueImpl<AttributeType>(attribute, id, value.get<NearestFieldType<AttributeType>>());
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}


const FlatDictionary::Attribute & FlatDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

PaddedPODArray<FlatDictionary::Key> FlatDictionary::getIds() const
{
    const auto ids_count = ext::size(loaded_ids);

    PaddedPODArray<Key> ids;
    for (auto idx : ext::range(0, ids_count))
        if (loaded_ids[idx])
            ids.push_back(idx);
    return ids;
}

BlockInputStreamPtr FlatDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<Key>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getIds(), column_names);
}

void registerDictionaryFlat(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'flat'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        return std::make_unique<FlatDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("flat", create_layout, false);
}


}
