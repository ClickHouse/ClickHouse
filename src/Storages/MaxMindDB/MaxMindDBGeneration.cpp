#include <Storages/MaxMindDB/MaxMindDBGeneration.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>

#include <array>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
extern const int INCORRECT_DATA;
extern const int TYPE_MISMATCH;
}

namespace
{
void checkMMDB(int status)
{
    if (status != MMDB_SUCCESS)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB: {}", MMDB_strerror(status));
}

using EntryDataList = std::unique_ptr<MMDB_entry_data_list_s, decltype(&MMDB_free_entry_data_list)>;

EntryDataList readList(MMDB_entry_s entry)
{
    MMDB_entry_data_list_s * list = nullptr;
    const auto status = MMDB_get_entry_data_list(&entry, &list);
    EntryDataList result(list, MMDB_free_entry_data_list);
    checkMMDB(status);
    return result;
}

const MMDB_entry_data_s & take(MMDB_entry_data_list_s *& cursor)
{
    if (!cursor)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB record ends before its declared number of values");
    const auto & data = cursor->entry_data;
    cursor = cursor->next;
    return data;
}

std::string_view mapKey(const MMDB_entry_data_s & data)
{
    if (data.type != MMDB_DATA_TYPE_UTF8_STRING || !data.has_data)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB map key must be a UTF-8 string");
    return {data.utf8_string, data.data_size};
}

void skipValue(MMDB_entry_data_list_s *& cursor)
{
    checkStackSize();
    const auto & data = take(cursor);
    const UInt64 children = data.type == MMDB_DATA_TYPE_MAP ? UInt64(data.data_size) * 2
        : data.type == MMDB_DATA_TYPE_ARRAY                 ? data.data_size
                                                            : 0;
    for (UInt64 i = 0; i < children; ++i)
        skipValue(cursor);
}

DataTypePtr optionalType(const DataTypePtr & type)
{
    /// ClickHouse represents absent arrays as empty arrays; `Nullable(Array)` is not supported.
    if (type->canBeInsideNullable())
        return makeNullable(type);
    return type;
}

DataTypePtr mergeTypes(const DataTypePtr & left, const DataTypePtr & right)
{
    checkStackSize();
    if (left->equals(*right))
        return left;
    const auto a = removeNullable(left);
    const auto b = removeNullable(right);
    DataTypePtr result;
    if (a->getTypeId() == TypeIndex::Nothing)
        result = b;
    else if (b->getTypeId() == TypeIndex::Nothing)
        result = a;
    else if (a->getTypeId() == TypeIndex::Tuple && b->getTypeId() == TypeIndex::Tuple)
    {
        const auto & first = assert_cast<const DataTypeTuple &>(*a);
        const auto & second = assert_cast<const DataTypeTuple &>(*b);
        Strings names = first.getElementNames();
        DataTypes types;
        types.reserve(first.getElements().size() + second.getElements().size());
        for (size_t i = 0; i < names.size(); ++i)
        {
            if (const auto position = second.tryGetPositionByName(names[i]))
                types.push_back(mergeTypes(first.getElement(i), second.getElement(*position)));
            else
                types.push_back(optionalType(first.getElement(i)));
        }
        for (size_t i = 0; i < second.getElementNames().size(); ++i)
        {
            if (!first.tryGetPositionByName(second.getElementNames()[i]))
            {
                names.push_back(second.getElementNames()[i]);
                types.push_back(optionalType(second.getElement(i)));
            }
        }
        result = std::make_shared<DataTypeTuple>(types, names);
    }
    else if (a->getTypeId() == TypeIndex::Array && b->getTypeId() == TypeIndex::Array)
    {
        result = std::make_shared<DataTypeArray>(
            mergeTypes(assert_cast<const DataTypeArray &>(*a).getNestedType(), assert_cast<const DataTypeArray &>(*b).getNestedType()));
    }
    else if ((isUInt64(a) && WhichDataType(b).isNativeInt()) || (isUInt64(b) && WhichDataType(a).isNativeInt()))
    {
        /// Preserve integer precision where the generic supertype policy avoids `Int128`.
        result = std::make_shared<DataTypeInt128>();
    }
    else
        result = getLeastSupertype({a, b}, true);
    return left->isNullable() || right->isNullable() ? optionalType(result) : result;
}

template <typename Type>
const DataTypePtr & scalarSchemaType()
{
    static const DataTypePtr type = std::make_shared<Type>();
    return type;
}

DataTypePtr inferType(MMDB_entry_data_list_s *& cursor)
{
    checkStackSize();
    const auto & data = take(cursor);
    switch (data.type)
    {
        case MMDB_DATA_TYPE_UTF8_STRING:
        case MMDB_DATA_TYPE_BYTES: return scalarSchemaType<DataTypeString>();
        case MMDB_DATA_TYPE_DOUBLE: return scalarSchemaType<DataTypeFloat64>();
        case MMDB_DATA_TYPE_FLOAT: return scalarSchemaType<DataTypeFloat32>();
        case MMDB_DATA_TYPE_INT32: return scalarSchemaType<DataTypeInt32>();
        case MMDB_DATA_TYPE_UINT16: return scalarSchemaType<DataTypeUInt16>();
        case MMDB_DATA_TYPE_UINT32: return scalarSchemaType<DataTypeUInt32>();
        case MMDB_DATA_TYPE_UINT64: return scalarSchemaType<DataTypeUInt64>();
        case MMDB_DATA_TYPE_UINT128: return scalarSchemaType<DataTypeUInt128>();
        case MMDB_DATA_TYPE_BOOLEAN: {
            static const DataTypePtr type = DataTypeFactory::instance().get("Bool");
            return type;
        }
        case MMDB_DATA_TYPE_ARRAY: {
            DataTypePtr nested = std::make_shared<DataTypeNothing>();
            for (UInt32 i = 0; i < data.data_size; ++i)
                nested = mergeTypes(nested, inferType(cursor));
            return std::make_shared<DataTypeArray>(nested);
        }
        case MMDB_DATA_TYPE_MAP: {
            Strings names;
            DataTypes types;
            names.reserve(data.data_size);
            types.reserve(data.data_size);
            for (UInt32 i = 0; i < data.data_size; ++i)
            {
                names.emplace_back(mapKey(take(cursor)));
                if (names.back().empty() || names.back().contains('\0'))
                    throw Exception(
                        ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "MaxMindDB map keys must be nonempty and contain no NUL bytes");
                types.push_back(inferType(cursor));
            }
            return std::make_shared<DataTypeTuple>(types, names);
        }
        default: throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported MaxMindDB data type {}", data.type);
    }
}

template <typename T, typename U>
void appendNumber(U value, IColumn & column)
{
    T converted{};
    if constexpr (std::is_floating_point_v<T>)
        converted = static_cast<T>(value);
    else if (!accurate::convertNumeric(value, converted))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB number is not representable in the declared column type");
    assert_cast<ColumnVector<T> &>(column).insertValue(converted);
}

template <typename T>
void appendNumber(const MMDB_entry_data_s & data, IColumn & column)
{
    switch (data.type)
    {
        case MMDB_DATA_TYPE_BOOLEAN: return appendNumber<T>(UInt8(data.boolean), column);
        case MMDB_DATA_TYPE_INT32: return appendNumber<T>(data.int32, column);
        case MMDB_DATA_TYPE_UINT16: return appendNumber<T>(data.uint16, column);
        case MMDB_DATA_TYPE_UINT32: return appendNumber<T>(data.uint32, column);
        case MMDB_DATA_TYPE_UINT64: return appendNumber<T>(data.uint64, column);
        case MMDB_DATA_TYPE_UINT128: return appendNumber<T>((UInt128(UInt64(data.uint128 >> 64)) << 64) | UInt64(data.uint128), column);
        case MMDB_DATA_TYPE_FLOAT: {
            if constexpr (std::is_floating_point_v<T>)
                return appendNumber<T>(data.float_value, column);
            break;
        }
        case MMDB_DATA_TYPE_DOUBLE: {
            if constexpr (std::is_floating_point_v<T>)
                return appendNumber<T>(data.double_value, column);
            break;
        }
        default: break;
    }
    throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB value has an incompatible numeric type");
}

/// This tree contains types and field names only; payload bytes go directly into the columns.
struct ValueDecoder
{
    DataTypePtr type;
    TypeIndex kind;
    bool is_bool;
    Strings names;
    std::vector<ValueDecoder> children;

    explicit ValueDecoder(DataTypePtr type_)
        : type(std::move(type_))
        , kind(type->getTypeId())
        , is_bool(type->getName() == "Bool")
    {
        checkStackSize();
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
            children.emplace_back(nullable->getNestedType());
        else if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
        {
            if (!tuple->hasExplicitNames() && !tuple->getElements().empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB maps require named Tuple elements");
            names = tuple->getElementNames();
            for (const auto & element : tuple->getElements())
                children.emplace_back(element);
        }
        else if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
            children.emplace_back(array->getNestedType());
        else if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        {
            if (map->getKeyType()->getTypeId() != TypeIndex::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB Map keys must have type String");
            children.emplace_back(map->getValueType());
        }
        else if (
            kind != TypeIndex::String && kind != TypeIndex::Nothing && !isInteger(type) && kind != TypeIndex::Float32
            && kind != TypeIndex::Float64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type {} is not supported for MaxMindDB payload columns", type->getName());
    }

    void missing(IColumn & column) const
    {
        if (kind == TypeIndex::Tuple)
        {
            auto & tuple = assert_cast<ColumnTuple &>(column);
            for (size_t i = 0; i < children.size(); ++i)
                children[i].missing(tuple.getColumn(i));
            if (children.empty())
                tuple.insertDefault();
            return;
        }
        if (kind != TypeIndex::Nullable && kind != TypeIndex::Array && kind != TypeIndex::Map)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "MaxMindDB field is missing but its declared type {} cannot represent missing values",
                type->getName());
        column.insertDefault();
    }

    void scalar(const MMDB_entry_data_s & data, IColumn & column) const
    {
        if (is_bool && data.type != MMDB_DATA_TYPE_BOOLEAN)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB Bool column requires a boolean value");
        switch (kind)
        {
            case TypeIndex::String: {
                if (data.type == MMDB_DATA_TYPE_UTF8_STRING)
                    assert_cast<ColumnString &>(column).insertData(data.utf8_string, data.data_size);
                else if (data.type == MMDB_DATA_TYPE_BYTES)
                    assert_cast<ColumnString &>(column).insertData(reinterpret_cast<const char *>(data.bytes), data.data_size);
                else
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB String column requires a string or bytes value");
                return;
            }
#define MAXMINDDB_NUMBER_CASE(T) \
    case TypeIndex::T: return appendNumber<T>(data, column);
                MAXMINDDB_NUMBER_CASE(UInt8)
                MAXMINDDB_NUMBER_CASE(UInt16)
                MAXMINDDB_NUMBER_CASE(UInt32)
                MAXMINDDB_NUMBER_CASE(UInt64)
                MAXMINDDB_NUMBER_CASE(UInt128)
                MAXMINDDB_NUMBER_CASE(UInt256)
                MAXMINDDB_NUMBER_CASE(Int8)
                MAXMINDDB_NUMBER_CASE(Int16)
                MAXMINDDB_NUMBER_CASE(Int32)
                MAXMINDDB_NUMBER_CASE(Int64)
                MAXMINDDB_NUMBER_CASE(Int128)
                MAXMINDDB_NUMBER_CASE(Int256)
                MAXMINDDB_NUMBER_CASE(Float32)
                MAXMINDDB_NUMBER_CASE(Float64)
#undef MAXMINDDB_NUMBER_CASE
            default: throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB value is incompatible with {}", type->getName());
        }
    }

    void list(MMDB_entry_data_list_s *& cursor, IColumn & column) const
    {
        checkStackSize();
        if (kind == TypeIndex::Nullable)
        {
            auto & nullable = assert_cast<ColumnNullable &>(column);
            children.front().list(cursor, nullable.getNestedColumn());
            nullable.getNullMapData().push_back(UInt8{0});
            return;
        }
        const auto & data = take(cursor);
        if (kind == TypeIndex::Array)
        {
            if (data.type != MMDB_DATA_TYPE_ARRAY)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB Array column requires an array");
            auto & array = assert_cast<ColumnArray &>(column);
            for (UInt32 i = 0; i < data.data_size; ++i)
                children.front().list(cursor, array.getData());
            array.getOffsets().push_back(array.getData().size());
        }
        else if (kind == TypeIndex::Map)
        {
            if (data.type != MMDB_DATA_TYPE_MAP)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB Map column requires a map");
            auto & array = assert_cast<ColumnMap &>(column).getNestedColumn();
            auto & tuple = assert_cast<ColumnTuple &>(array.getData());
            for (UInt32 i = 0; i < data.data_size; ++i)
            {
                const auto key = mapKey(take(cursor));
                assert_cast<ColumnString &>(tuple.getColumn(0)).insertData(key.data(), key.size());
                children.front().list(cursor, tuple.getColumn(1));
            }
            array.getOffsets().push_back(tuple.size());
        }
        else if (kind == TypeIndex::Tuple)
        {
            if (data.type != MMDB_DATA_TYPE_MAP)
                throw Exception(ErrorCodes::TYPE_MISMATCH, "MaxMindDB Tuple column requires a map");
            auto & tuple = assert_cast<ColumnTuple &>(column);
            const size_t old_size = tuple.size();
            for (UInt32 i = 0; i < data.data_size; ++i)
            {
                const auto key = mapKey(take(cursor));
                const auto it = std::find(names.begin(), names.end(), key);
                if (it == names.end())
                    skipValue(cursor);
                else
                {
                    const size_t position = it - names.begin();
                    if (tuple.getColumn(position).size() != old_size)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB map contains duplicate keys");
                    children[position].list(cursor, tuple.getColumn(position));
                }
            }
            for (size_t i = 0; i < children.size(); ++i)
                if (tuple.getColumn(i).size() == old_size)
                    children[i].missing(tuple.getColumn(i));
            if (children.empty())
                tuple.insertDefault();
        }
        else
            scalar(data, column);
    }

    void value(MMDB_entry_s entry, const MMDB_entry_data_s & data, IColumn & column) const
    {
        if (kind == TypeIndex::Nullable)
        {
            auto & nullable = assert_cast<ColumnNullable &>(column);
            children.front().value(entry, data, nullable.getNestedColumn());
            nullable.getNullMapData().push_back(UInt8{0});
        }
        else if (kind == TypeIndex::Tuple || kind == TypeIndex::Map || kind == TypeIndex::Array)
        {
            entry.offset = data.offset;
            auto values = readList(entry);
            auto * cursor = values.get();
            list(cursor, column);
            if (cursor)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected trailing values in MaxMindDB record");
        }
        else
            scalar(data, column);
    }
};
}

struct MaxMindDBColumnDecoder::Column
{
    NameAndTypePair description;
    Strings path;
    std::vector<const char *> path_pointers;
    String extract_subcolumn;
    ValueDecoder decoder;
    struct CachedValue
    {
        UInt32 offset;
        size_t row;
    };
    std::array<CachedValue, 128> cache{};
    bool use_cache = false;

    explicit Column(const NameAndTypePair & column)
        : description(column)
        , decoder(column.type)
    {
        path.push_back(column.getNameInStorage());
        if (column.isSubcolumn())
        {
            auto parent = column.getTypeInStorage();
            String remaining = column.getSubcolumnName();
            while (!remaining.empty())
            {
                parent = removeNullable(parent);
                const auto * tuple = typeid_cast<const DataTypeTuple *>(parent.get());
                const size_t end = remaining.find('.');
                const String name = remaining.substr(0, end);
                const auto position = tuple ? tuple->tryGetPositionByName(name) : std::nullopt;
                if (!position)
                {
                    extract_subcolumn = column.getSubcolumnName();
                    path.resize(1);
                    decoder = ValueDecoder(column.getTypeInStorage());
                    break;
                }
                path.push_back(name);
                parent = tuple->getElement(*position);
                remaining = end == String::npos ? String{} : remaining.substr(end + 1);
            }
        }
        path_pointers.reserve(path.size() + 1);
        for (const auto & element : path)
        {
            if (element.contains('\0'))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB field names must not contain NUL bytes");
            path_pointers.push_back(element.c_str());
        }
        path_pointers.push_back(nullptr);
    }

    void startBlock(size_t rows)
    {
        const auto kind = removeNullable(decoder.type)->getTypeId();
        use_cache = rows >= cache.size() && (kind == TypeIndex::Tuple || kind == TypeIndex::Map || kind == TypeIndex::Array);
        if (use_cache)
            for (auto & value : cache)
                value.row = std::numeric_limits<size_t>::max();
    }

    void append(MMDB_entry_s entry, IColumn & column)
    {
        MMDB_entry_data_s data{};
        const auto status = MMDB_aget_value(&entry, &data, path_pointers.data());
        if (status != MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR)
            checkMMDB(status);
        CachedValue * cached = nullptr;
        if (use_cache && data.has_data)
        {
            /// Shared MMDB subtrees can belong to different IP records. Reuse an already decoded row
            /// within this block; `IColumn` owns the copied result and the generation remains pinned.
            cached = &cache[intHash32<0>(data.offset) & (cache.size() - 1)];
            if (cached->row != std::numeric_limits<size_t>::max() && cached->offset == data.offset)
            {
                column.insertFrom(column, cached->row);
                return;
            }
        }
        const auto row = column.size();
        if (extract_subcolumn.empty())
        {
            if (data.has_data)
                decoder.value(entry, data, column);
            else
                decoder.missing(column);
        }
        else
        {
            auto root = decoder.type->createColumn();
            if (data.has_data)
                decoder.value(entry, data, *root);
            else
                decoder.missing(*root);
            const auto extracted = decoder.type->getSubcolumn(extract_subcolumn, std::move(root));
            column.insertFrom(*extracted, 0);
        }
        if (cached)
            *cached = {data.offset, row};
    }
};

MaxMindDBColumnDecoder::MaxMindDBColumnDecoder(const NamesAndTypesList & columns)
{
    projections.reserve(columns.size());
    for (const auto & column : columns)
    {
        if (!first_projection && column.name != "ip" && !MaxMindDBGeneration::isMetadataColumn(column.name))
            first_projection = projections.size();
        projections.push_back(
            column.name == "ip" || MaxMindDBGeneration::isMetadataColumn(column.name) ? nullptr : std::make_unique<Column>(column));
    }
}

MaxMindDBColumnDecoder::~MaxMindDBColumnDecoder() = default;

void MaxMindDBColumnDecoder::startBlock(size_t rows)
{
    reuse_records = first_projection.has_value() && rows >= records.size();
    if (reuse_records)
        for (auto & record : records)
            record.row = std::numeric_limits<size_t>::max();
    for (const auto & projection : projections)
        if (projection)
            projection->startBlock(rows);
}

void MaxMindDBColumnDecoder::append(MMDB_entry_s entry, MutableColumns & columns)
{
    CachedRecord * cached = nullptr;
    size_t row = 0;
    if (reuse_records)
    {
        /// Different lookup keys can resolve to the same record by longest-prefix matching.
        /// Row indexes stay valid across column reallocations, and are discarded at every block.
        cached = &records[intHash32<0>(entry.offset) & (records.size() - 1)];
        if (cached->row != std::numeric_limits<size_t>::max() && cached->offset == entry.offset)
        {
            for (size_t i = 0; i < projections.size(); ++i)
                if (projections[i])
                    columns[i]->insertFrom(*columns[i], cached->row);
            return;
        }
        row = columns[*first_projection]->size();
    }
    for (size_t i = 0; i < projections.size(); ++i)
        if (projections[i])
            projections[i]->append(entry, *columns[i]);
    if (cached)
        *cached = {entry.offset, row};
}

Block MaxMindDBColumnDecoder::sampleBlock() const
{
    Block result;
    for (const auto & projection : projections)
        if (projection)
            result.insert({projection->description.type->createColumn(), projection->description.type, projection->description.name});
    return result;
}

bool MaxMindDBGeneration::isMetadataColumn(std::string_view name)
{
    return name == metadata_column_name || name.starts_with("_mmdb_metadata.");
}

DataTypePtr MaxMindDBGeneration::getMetadataType()
{
    static const auto type = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeUInt32>(),
            std::make_shared<DataTypeUInt16>(),
            std::make_shared<DataTypeUInt16>(),
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            std::make_shared<DataTypeUInt16>(),
            std::make_shared<DataTypeUInt16>(),
            std::make_shared<DataTypeUInt64>(),
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())},
        Names{
            "node_count",
            "record_size",
            "ip_version",
            "database_type",
            "languages",
            "binary_format_major_version",
            "binary_format_minor_version",
            "build_epoch",
            "description"});
    return type;
}

MaxMindDBGeneration::MaxMindDBGeneration(std::unique_ptr<MaxMindDBFile> file_)
    : file(std::move(file_))
{
    const auto status = MMDB_open(file->path.c_str(), MMDB_MODE_MMAP, &mmdb);
    checkMMDB(status);
    SCOPE_FAIL({ MMDB_close(&mmdb); });
    if (mmdb.metadata.ip_version != 4 && mmdb.metadata.ip_version != 6)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB metadata must specify IP version 4 or 6");

    const auto & metadata = mmdb.metadata;
    Array languages;
    languages.reserve(metadata.languages.count);
    for (size_t i = 0; i < metadata.languages.count; ++i)
        languages.emplace_back(String(metadata.languages.names[i]));
    Map description;
    description.reserve(metadata.description.count);
    for (size_t i = 0; i < metadata.description.count; ++i)
    {
        const auto & item = *metadata.description.descriptions[i];
        description.emplace_back(Tuple{String(item.language), String(item.description)});
    }
    auto column = getMetadataType()->createColumn();
    column->insert(
        Tuple{
            UInt64(metadata.node_count),
            UInt64(metadata.record_size),
            UInt64(metadata.ip_version),
            String(metadata.database_type),
            std::move(languages),
            UInt64(metadata.binary_format_major_version),
            UInt64(metadata.binary_format_minor_version),
            metadata.build_epoch,
            std::move(description)});
    metadata_column = std::move(column);
}

MaxMindDBGeneration::~MaxMindDBGeneration()
{
    MMDB_close(&mmdb);
}

ColumnPtr MaxMindDBGeneration::getMetadataColumn(const String & name) const
{
    if (name == metadata_column_name)
        return metadata_column;
    return getMetadataType()->getSubcolumn(name.substr(std::string_view(metadata_column_name).size() + 1), metadata_column);
}

void MaxMindDBGeneration::forEachRecord(const std::function<void(MMDB_entry_s)> & visitor) const
{
    UnorderedSetWithMemoryTracking<UInt32> offsets;
    for (UInt32 node_number = 0; node_number < mmdb.metadata.node_count; ++node_number)
    {
        MMDB_search_node_s node{};
        checkMMDB(MMDB_read_node(&mmdb, node_number, &node));
        const auto visit = [&](UInt8 kind, MMDB_entry_s entry)
        {
            if (kind == MMDB_RECORD_TYPE_INVALID)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid MaxMindDB search tree record");
            if (kind == MMDB_RECORD_TYPE_DATA && offsets.insert(entry.offset).second)
                visitor(entry);
        };
        visit(node.left_record_type, node.left_record_entry);
        visit(node.right_record_type, node.right_record_entry);
    }
}

ColumnsDescription MaxMindDBGeneration::inferSchema() const
{
    DataTypePtr merged;
    forEachRecord(
        [&](MMDB_entry_s entry)
        {
            auto list = readList(entry);
            auto * cursor = list.get();
            auto type = inferType(cursor);
            if (type->getTypeId() != TypeIndex::Tuple || cursor)
                throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "MaxMindDB root records must be maps");
            merged = merged ? mergeTypes(merged, type) : type;
        });
    if (!merged)
        throw Exception(ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE, "Cannot infer MaxMindDB schema from a database with no data records");
    const auto & tuple = assert_cast<const DataTypeTuple &>(*merged);
    NamesAndTypesList columns;
    columns.emplace_back(
        "ip", mmdb.metadata.ip_version == 4 ? DataTypePtr(std::make_shared<DataTypeIPv4>()) : std::make_shared<DataTypeIPv6>());
    for (size_t i = 0; i < tuple.getElements().size(); ++i)
    {
        const auto & name = tuple.getElementNames()[i];
        if (name == "ip")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB root payload field ip conflicts with the reserved lookup column ip");
        if (isMetadataColumn(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB root payload field {} is reserved for database metadata", name);
        columns.emplace_back(name, tuple.getElement(i));
    }
    return ColumnsDescription(columns);
}

void MaxMindDBGeneration::validateSchema(const ColumnsDescription & columns) const
{
    for (const auto & column : columns)
        if (isMetadataColumn(column.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB column {} is reserved for database metadata", column.name);
    if (!columns.has("ip"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB requires an ip column of type IPv4 or IPv6");
    const auto key_type = columns.getPhysical("ip").type->getTypeId();
    if (key_type != TypeIndex::IPv4 && key_type != TypeIndex::IPv6)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB ip column must have type IPv4 or IPv6");
    if (key_type == TypeIndex::IPv6 && mmdb.metadata.ip_version == 4)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB IPv4 databases require an IPv4 lookup column");
    if (!columns.getAliases().empty() || !columns.getMaterialized().empty() || !columns.getDefaults().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB columns must not have default, materialized, or alias expressions");

    auto payload_columns = columns.getOrdinary();
    payload_columns.remove_if([](const auto & column) { return column.name == "ip"; });
    MaxMindDBColumnDecoder decoder(payload_columns);
    auto decoded = decoder.sampleBlock().cloneEmptyColumns();
    forEachRecord(
        [&](MMDB_entry_s entry)
        {
            MMDB_entry_data_s root{};
            checkMMDB(MMDB_get_value(&entry, &root, nullptr));
            if (root.type != MMDB_DATA_TYPE_MAP)
                throw Exception(ErrorCodes::INCORRECT_DATA, "MaxMindDB root records must be maps");
            MMDB_entry_data_s reserved{};
            const auto status = MMDB_get_value(&entry, &reserved, "ip", nullptr);
            if (status != MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR)
                checkMMDB(status);
            if (reserved.has_data)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaxMindDB root payload field ip conflicts with the reserved lookup column ip");
            reserved = {};
            const auto metadata_status = MMDB_get_value(&entry, &reserved, metadata_column_name, nullptr);
            if (metadata_status != MMDB_LOOKUP_PATH_DOES_NOT_MATCH_DATA_ERROR)
                checkMMDB(metadata_status);
            if (reserved.has_data)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "MaxMindDB root payload field {} is reserved for database metadata", metadata_column_name);
            decoder.append(entry, decoded);
            for (auto & column : decoded)
                column->popBack(1);
        });
}
}
