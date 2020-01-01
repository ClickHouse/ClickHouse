#include "SSDCacheDictionary.h"

#include <Columns/ColumnsNumber.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/typeid_cast.h>
#include <DataStreams/IBlockInputStream.h>
#include <ext/chrono_io.h>
#include <ext/map.h>
#include <ext/range.h>
#include <ext/size.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_SMALL_BUFFER_SIZE;
}

CachePartition::CachePartition(const std::string & file_name, const Block & header, size_t buffer_size)
    : file_name(file_name), buffer_size(buffer_size), out_file(file_name, buffer_size), header(header), buffer(header.cloneEmptyColumns())
{
}

void CachePartition::appendBlock(const Block & block)
{
    size_t bytes = 0;
    const auto new_columns = block.getColumns();
    if (new_columns.size() != header.columns())
    {
        throw Exception("Wrong size of block in BlockFile::appendBlock(). It's a bug.", ErrorCodes::TYPE_MISMATCH);
    }

    const auto id_column = typeid_cast<const ColumnUInt64 *>(new_columns.front().get());
    if (!id_column)
        throw Exception{"id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};

    size_t start_size = buffer.front()->size();
    for (size_t i = 0; i < header.columns(); ++i)
    {
        buffer[i]->insertRangeFrom(*new_columns[i], 0, new_columns[i]->size());
        bytes += buffer[i]->byteSize();
    }

    const auto & ids = id_column->getData();
    for (size_t i = 0; i < new_columns.size(); ++i)
    {
        key_to_file_offset[ids[i]] = start_size + i;
    }

    if (bytes >= buffer_size)
    {
        flush();
    }
}

void CachePartition::flush()
{
    const auto id_column = typeid_cast<const ColumnUInt64 *>(buffer.front().get());
    if (!id_column)
        throw Exception{"id column has type different from UInt64.", ErrorCodes::TYPE_MISMATCH};
    const auto & ids = id_column->getData();

    key_to_file_offset[ids[0]] = out_file.getPositionInFile() + (1ULL << FILE_OFFSET_SIZE);
    size_t prev_size = 0;
    for (size_t row = 0; row < buffer.front()->size(); ++row)
    {
        key_to_file_offset[ids[row]] = key_to_file_offset[ids[row ? row - 1 : 0]] + prev_size;
        prev_size = 0;
        for (size_t col = 0; col < header.columns(); ++col)
        {
            const auto & column = buffer[col];
            const auto & type = header.getByPosition(col).type;
            type->serializeBinary(*column, row, out_file);
            if (type->getTypeId() != TypeIndex::String) {
                prev_size += column->sizeOfValueIfFixed();
            } else {
                prev_size += column->getDataAt(row).size + sizeof(UInt64);
            }
        }
    }

    if (out_file.hasPendingData()) {
        out_file.sync();
    }

    buffer = header.cloneEmptyColumns();
}

SSDCacheDictionary::SSDCacheDictionary(
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    const std::string & path,
    const size_t partition_max_size)
    : name(name_)
    , dict_struct(dict_struct_)
    , source_ptr(std::move(source_ptr_))
    , dict_lifetime(dict_lifetime_)
    , storage(path, partition_max_size)
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{name + ": source cannot be used with CacheDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = std::get<TYPE>(attributes[index].null_value); \
\
        getItemsNumberImpl<TYPE, TYPE>( \
            attribute_name, \
            ids, \
            out, \
            [&](const size_t) { return null_value; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsNumberImpl<TYPE, TYPE>( \
            attribute_name, \
            ids, \
            out, \
            [&](const size_t row) { return def[row]; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

#define DECLARE(TYPE) \
    void SSDCacheDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto index = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[index].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsNumberImpl<TYPE, TYPE>( \
            attribute_name, \
            ids, \
            out, \
            [&](const size_t) { return def; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

template <typename AttributeType, typename OutputType, typename DefaultGetter>
void SSDCacheDictionary::getItemsNumberImpl(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<OutputType> & out, DefaultGetter && get_default) const
{
    std::unordered_map<Key, std::vector<size_t>> not_found_ids;
    storage.getValue(attribute_name, ids, out, not_found_ids);
    if (not_found_ids.empty())
        return;

    std::vector<Key> required_ids(not_found_ids.size());
    std::transform(std::begin(not_found_ids), std::end(not_found_ids), std::begin(required_ids), [](auto & pair) { return pair.first; });

    update(
            required_ids,
            [&](const auto id, const auto & attribute_value)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = static_cast<OutputType>(attribute_value);
            },
            [&](const auto id)
            {
                for (const size_t row : not_found_ids[id])
                    out[row] = get_default(row);
            });
}

void SSDCacheDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    const auto null_value = StringRef{std::get<String>(attribute.null_value)};

    getItemsString(attribute_name, ids, out, [&](const size_t) { return null_value; });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsString(attribute_name, ids, out, [&](const size_t row) { return def->getDataAt(row); });
}

void SSDCacheDictionary::getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    auto & attribute = getAttribute(attribute_name);
    checkAttributeType(name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsString(attribute_name, ids, out, [&](const size_t) { return StringRef{def}; });
}

template <typename DefaultGetter>
void SSDCacheDictionary::getItemsString(const std::string & attribute_name, const PaddedPODArray<Key> & ids,
        ColumnString * out, DefaultGetter && get_default) const
{
    UNUSED(attribute_name);
    UNUSED(ids);
    UNUSED(out);
    UNUSED(get_default);
}

size_t SSDCacheDictionary::getAttributeIndex(const std::string & attr_name) const
{
    auto it = attribute_index_by_name.find(attr_name);
    if (it == std::end(attribute_index_by_name))
        throw  Exception{"Attribute `" + name + "` does not exist.", ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

SSDCacheDictionary::Attribute & SSDCacheDictionary::getAttribute(const std::string & attr_name)
{
    return attributes[getAttributeIndex(attr_name)];
}

const SSDCacheDictionary::Attribute & SSDCacheDictionary::getAttribute(const std::string & attr_name) const
{
    return attributes[getAttributeIndex(attr_name)];
}

template <typename T>
SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithTypeImpl(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}};
    attr.null_value = static_cast<T>(null_value.get<NearestFieldType<T>>());
    bytes_allocated += sizeof(T);
    return attr;
}

template <>
SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithTypeImpl<String>(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}};
    attr.null_value = null_value.get<String>();
    bytes_allocated += sizeof(StringRef);
    //if (!string_arena)
    //    string_arena = std::make_unique<ArenaWithFreeLists>();
    return attr;
}

SSDCacheDictionary::Attribute SSDCacheDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
#define DISPATCH(TYPE) \
case AttributeUnderlyingType::ut##TYPE: \
    return createAttributeWithTypeImpl<TYPE>(type, null_value);

        DISPATCH(UInt8)
        DISPATCH(UInt16)
        DISPATCH(UInt32)
        DISPATCH(UInt64)
        DISPATCH(UInt128)
        DISPATCH(Int8)
        DISPATCH(Int16)
        DISPATCH(Int32)
        DISPATCH(Int64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
        DISPATCH(Float32)
        DISPATCH(Float64)
        DISPATCH(String)
#undef DISPATCH
    }
}

void SSDCacheDictionary::createAttributes()
{
    attributes.resize(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        const auto & attribute = dict_struct.attributes[i];

        attribute_index_by_name.emplace(attribute.name, i);
        attributes.push_back(createAttributeWithType(attribute.type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

}
