#include "config.h"

#if USE_MONGODB
#include "MongoDBPocoLegacySource.h"

#include <string>
#include <vector>

#include <Poco/MongoDB/Array.h>
#include <Poco/MongoDB/Binary.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/OpMsgCursor.h>
#include <Poco/MongoDB/ObjectId.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include "base/types.h"
#include <base/range.h>
#include <Poco/URI.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>

// only after poco
// naming conflict:
// Poco/MongoDB/BSONWriter.h:54: void writeCString(const std::string & value);
// src/IO/WriteHelpers.h:146 #define writeCString(s, buf)
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_TYPE;
    extern const int MONGODB_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;
    using ObjectId = Poco::MongoDB::ObjectId;
    using MongoArray = Poco::MongoDB::Array;
    using MongoUUID = Poco::MongoDB::Binary::Ptr;


    UUID parsePocoUUID(const Poco::UUID & src)
    {
        UUID uuid;

        std::array<Poco::UInt8, 6> src_node = src.getNode();
        UInt64 node = 0;
        node |= UInt64(src_node[0]) << 40;
        node |= UInt64(src_node[1]) << 32;
        node |= UInt64(src_node[2]) << 24;
        node |= UInt64(src_node[3]) << 16;
        node |= UInt64(src_node[4]) << 8;
        node |= src_node[5];

        UUIDHelpers::getHighBytes(uuid) = UInt64(src.getTimeLow()) << 32 | UInt32(src.getTimeMid() << 16 | src.getTimeHiAndVersion());
        UUIDHelpers::getLowBytes(uuid) = UInt64(src.getClockSeq()) << 48 | node;

        return uuid;
    }

    template <typename T>
    Field getNumber(const Poco::MongoDB::Element & value, const std::string & name)
    {
        switch (value.type())
        {
            case Poco::MongoDB::ElementTraits<Int32>::TypeId:
                return static_cast<T>(static_cast<const Poco::MongoDB::ConcreteElement<Int32> &>(value).value());
            case Poco::MongoDB::ElementTraits<Poco::Int64>::TypeId:
                return static_cast<T>(static_cast<const Poco::MongoDB::ConcreteElement<Poco::Int64> &>(value).value());
            case Poco::MongoDB::ElementTraits<Float64>::TypeId:
                return static_cast<T>(static_cast<const Poco::MongoDB::ConcreteElement<Float64> &>(value).value());
            case Poco::MongoDB::ElementTraits<bool>::TypeId:
                return static_cast<T>(static_cast<const Poco::MongoDB::ConcreteElement<bool> &>(value).value());
            case Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId:
                return Field();
            case Poco::MongoDB::ElementTraits<String>::TypeId:
                return parse<T>(static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value());
            default:
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected a number, got type id = {} for column {}",
                    toString(value.type()), name);
        }
    }

    void prepareMongoDBArrayInfo(
        std::unordered_map<size_t, MongoDBPocoLegacyArrayInfo> & array_info, size_t column_idx, const DataTypePtr data_type)
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(data_type.get());
        auto nested = array_type->getNestedType();

        size_t count_dimensions = 1;
        while (isArray(nested))
        {
            ++count_dimensions;
            nested = assert_cast<const DataTypeArray *>(nested.get())->getNestedType();
        }

        Field default_value = nested->getDefault();
        if (nested->isNullable())
            nested = assert_cast<const DataTypeNullable *>(nested.get())->getNestedType();

        WhichDataType which(nested);
        std::function<Field(const Poco::MongoDB::Element & value, const std::string & name)> parser;

        if (which.isUInt8())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<UInt8>(value, name); };
        else if (which.isUInt16())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<UInt16>(value, name); };
        else if (which.isUInt32())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<UInt32>(value, name); };
        else if (which.isUInt64())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<UInt64>(value, name); };
        else if (which.isInt8())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Int8>(value, name); };
        else if (which.isInt16())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Int16>(value, name); };
        else if (which.isInt32())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Int32>(value, name); };
        else if (which.isInt64())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Int64>(value, name); };
        else if (which.isFloat32())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Float32>(value, name); };
        else if (which.isFloat64())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field { return getNumber<Float64>(value, name); };
        else if (which.isString() || which.isFixedString())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field
            {
                if (value.type() == Poco::MongoDB::ElementTraits<ObjectId::Ptr>::TypeId)
                {
                    String string_id = value.toString();
                    return Field(string_id.data(), string_id.size());
                }
                else if (value.type() == Poco::MongoDB::ElementTraits<String>::TypeId)
                {
                    String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                    return Field(string.data(), string.size());
                }

                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String, got type id = {} for column {}",
                                toString(value.type()), name);
            };
        else if (which.isDate())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                return static_cast<UInt16>(DateLUT::instance().toDayNum(
                    static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime()));
            };
        else if (which.isDateTime())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                return static_cast<UInt32>(static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime());
            };
        else if (which.isUUID())
            parser = [](const Poco::MongoDB::Element & value, const std::string & name) -> Field
            {
                if (value.type() == Poco::MongoDB::ElementTraits<String>::TypeId)
                {
                    String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                    return parse<UUID>(string);
                }
                else if (value.type() == Poco::MongoDB::ElementTraits<MongoUUID>::TypeId)
                {
                    const Poco::UUID & poco_uuid = static_cast<const Poco::MongoDB::ConcreteElement<MongoUUID> &>(value).value()->uuid();
                    return parsePocoUUID(poco_uuid);
                }
                else
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String/UUID, got type id = {} for column {}",
                                        toString(value.type()), name);
            };
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Type conversion to {} is not supported", nested->getName());

        array_info[column_idx] = {count_dimensions, default_value, parser};
    }

    template <typename T>
    void insertNumber(IColumn & column, const Poco::MongoDB::Element & value, const std::string & name)
    {
        switch (value.type())
        {
            case Poco::MongoDB::ElementTraits<Int32>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().push_back(
                    static_cast<const Poco::MongoDB::ConcreteElement<Int32> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<Poco::Int64>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().push_back(
                    static_cast<T>(static_cast<const Poco::MongoDB::ConcreteElement<Poco::Int64> &>(value).value()));
                break;
            case Poco::MongoDB::ElementTraits<Float64>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().push_back(static_cast<T>(
                    static_cast<const Poco::MongoDB::ConcreteElement<Float64> &>(value).value()));
                break;
            case Poco::MongoDB::ElementTraits<bool>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().push_back(
                    static_cast<const Poco::MongoDB::ConcreteElement<bool> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().emplace_back();
                break;
            case Poco::MongoDB::ElementTraits<String>::TypeId:
                assert_cast<ColumnVector<T> &>(column).getData().push_back(
                    parse<T>(static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value()));
                break;
            default:
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected a number, got type id = {} for column {}",
                    toString(value.type()), name);
        }
    }

    void insertValue(
        IColumn & column,
        const ValueType type,
        const Poco::MongoDB::Element & value,
        const std::string & name,
        std::unordered_map<size_t, MongoDBPocoLegacyArrayInfo> & array_info,
        size_t idx)
    {
        switch (type)
        {
            case ValueType::vtUInt8:
                insertNumber<UInt8>(column, value, name);
                break;
            case ValueType::vtUInt16:
                insertNumber<UInt16>(column, value, name);
                break;
            case ValueType::vtUInt32:
                insertNumber<UInt32>(column, value, name);
                break;
            case ValueType::vtUInt64:
                insertNumber<UInt64>(column, value, name);
                break;
            case ValueType::vtInt8:
                insertNumber<Int8>(column, value, name);
                break;
            case ValueType::vtInt16:
                insertNumber<Int16>(column, value, name);
                break;
            case ValueType::vtInt32:
                insertNumber<Int32>(column, value, name);
                break;
            case ValueType::vtInt64:
                insertNumber<Int64>(column, value, name);
                break;
            case ValueType::vtFloat32:
                insertNumber<Float32>(column, value, name);
                break;
            case ValueType::vtFloat64:
                insertNumber<Float64>(column, value, name);
                break;

            case ValueType::vtEnum8:
            case ValueType::vtEnum16:
            case ValueType::vtString:
            {
                if (value.type() == Poco::MongoDB::ElementTraits<ObjectId::Ptr>::TypeId)
                {
                    std::string string_id = value.toString();
                    assert_cast<ColumnString &>(column).insertData(string_id.data(), string_id.size());
                    break;
                }
                else if (value.type() == Poco::MongoDB::ElementTraits<String>::TypeId)
                {
                    String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                    assert_cast<ColumnString &>(column).insertData(string.data(), string.size());
                    break;
                }

                throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String, got type id = {} for column {}",
                                toString(value.type()), name);
            }

            case ValueType::vtDate:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                assert_cast<ColumnUInt16 &>(column).getData().push_back(static_cast<UInt16>(DateLUT::instance().toDayNum(
                    static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime())));
                break;
            }

            case ValueType::vtDateTime:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                assert_cast<ColumnUInt32 &>(column).getData().push_back(
                    static_cast<UInt32>(static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime()));
                break;
            }
            case ValueType::vtUUID:
            {
                if (value.type() == Poco::MongoDB::ElementTraits<String>::TypeId)
                {
                    String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                    assert_cast<ColumnUUID &>(column).getData().push_back(parse<UUID>(string));
                }
                else if (value.type() == Poco::MongoDB::ElementTraits<MongoUUID>::TypeId)
                {
                    const Poco::UUID & poco_uuid = static_cast<const Poco::MongoDB::ConcreteElement<MongoUUID> &>(value).value()->uuid();
                    UUID uuid = parsePocoUUID(poco_uuid);
                    assert_cast<ColumnUUID &>(column).getData().push_back(uuid);
                }
                else
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String/UUID, got type id = {} for column {}",
                                        toString(value.type()), name);
                break;
            }
            case ValueType::vtArray:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<MongoArray::Ptr>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Array, got type id = {} for column {}",
                                    toString(value.type()), name);

                size_t expected_dimensions = array_info[idx].num_dimensions;
                const auto parse_value = array_info[idx].parser;
                std::vector<Row> dimensions(expected_dimensions + 1);

                auto array = static_cast<const Poco::MongoDB::ConcreteElement<MongoArray::Ptr> &>(value).value();

                std::vector<std::pair<const Poco::MongoDB::Element *, size_t>> arrays;
                arrays.emplace_back(&value, 0);

                while (!arrays.empty())
                {
                    size_t dimension_idx = arrays.size() - 1;

                    if (dimension_idx + 1 > expected_dimensions)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got more dimensions than expected");

                    auto [parent_ptr, child_idx] = arrays.back();
                    auto parent = static_cast<const Poco::MongoDB::ConcreteElement<MongoArray::Ptr> &>(*parent_ptr).value();

                    if (child_idx >= parent->size())
                    {
                        arrays.pop_back();

                        if (dimension_idx == 0)
                            break;

                        dimensions[dimension_idx].emplace_back(Array(dimensions[dimension_idx + 1].begin(), dimensions[dimension_idx + 1].end()));
                        dimensions[dimension_idx + 1].clear();

                        continue;
                    }

                    Poco::MongoDB::Element::Ptr child = parent->get(static_cast<int>(child_idx));
                    arrays.back().second += 1;

                    if (child->type() == Poco::MongoDB::ElementTraits<MongoArray::Ptr>::TypeId)
                    {
                        arrays.emplace_back(child.get(), 0);
                    }
                    else if (child->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                    {
                        if (dimension_idx + 1 == expected_dimensions)
                            dimensions[dimension_idx + 1].emplace_back(array_info[idx].default_value);
                        else
                            dimensions[dimension_idx + 1].emplace_back(Array());
                    }
                    else if (dimension_idx + 1 == expected_dimensions)
                    {
                        dimensions[dimension_idx + 1].emplace_back(parse_value(*child, name));
                    }
                    else
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Got less dimensions than expected. ({} instead of {})", dimension_idx + 1, expected_dimensions);
                    }
                }

                assert_cast<ColumnArray &>(column).insert(Array(dimensions[1].begin(), dimensions[1].end()));
                break;

            }
            default:
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Value of unsupported type: {}", column.getName());
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


bool isMongoDBWireProtocolOld(Poco::MongoDB::Connection & connection_, const std::string & database_name_)
{
    Poco::MongoDB::Database db(database_name_);
    Poco::MongoDB::Document::Ptr doc = db.queryServerHello(connection_, false);

    if (doc->exists("maxWireVersion"))
    {
        auto wire_version = doc->getInteger("maxWireVersion");
        return wire_version < Poco::MongoDB::Database::WireVersion::VER_36;
    }

    doc = db.queryServerHello(connection_, true);
    if (doc->exists("maxWireVersion"))
    {
        auto wire_version = doc->getInteger("maxWireVersion");
        return wire_version < Poco::MongoDB::Database::WireVersion::VER_36;
    }

    return true;
}


MongoDBPocoLegacyCursor::MongoDBPocoLegacyCursor(
    const std::string & database,
    const std::string & collection,
    const Block & sample_block_to_select,
    const Poco::MongoDB::Document & query,
    Poco::MongoDB::Connection & connection)
    : is_wire_protocol_old(isMongoDBWireProtocolOld(connection, database))
{
    Poco::MongoDB::Document projection;

    /// Looks like selecting _id column is implicit by default.
    if (!sample_block_to_select.has("_id"))
        projection.add("_id", 0);

    for (const auto & column : sample_block_to_select)
        projection.add(column.name, 1);

    if (is_wire_protocol_old)
    {
        old_cursor = std::make_unique<Poco::MongoDB::Cursor>(database, collection);
        old_cursor->query().selector() = query;
        old_cursor->query().returnFieldSelector() = projection;
    }
    else
    {
        new_cursor = std::make_unique<Poco::MongoDB::OpMsgCursor>(database, collection);
        new_cursor->query().setCommandName(Poco::MongoDB::OpMsgMessage::CMD_FIND);
        new_cursor->query().body().addNewDocument("filter") = query;
        new_cursor->query().body().addNewDocument("projection") = projection;
    }
}

Poco::MongoDB::Document::Vector MongoDBPocoLegacyCursor::nextDocuments(Poco::MongoDB::Connection & connection)
{
    if (is_wire_protocol_old)
    {
        auto response = old_cursor->next(connection);
        cursor_id = response.cursorID();
        return std::move(response.documents());
    }
    else
    {
        auto response = new_cursor->next(connection);
        cursor_id = new_cursor->cursorID();
        return std::move(response.documents());
    }
}

Int64 MongoDBPocoLegacyCursor::cursorID() const
{
    return cursor_id;
}


MongoDBPocoLegacySource::MongoDBPocoLegacySource(
    std::shared_ptr<Poco::MongoDB::Connection> & connection_,
    const String & database_name_,
    const String & collection_name_,
    const Poco::MongoDB::Document & query_,
    const Block & sample_block,
    UInt64 max_block_size_)
    : ISource(sample_block.cloneEmpty())
    , connection(connection_)
    , cursor(database_name_, collection_name_, sample_block, query_, *connection_)
    , max_block_size{max_block_size_}
{
    description.init(sample_block);

    for (const auto idx : collections::range(0, description.sample_block.columns()))
        if (description.types[idx].first == ExternalResultDescription::ValueType::vtArray)
            prepareMongoDBArrayInfo(array_info, idx, description.sample_block.getByPosition(idx).type);
}


MongoDBPocoLegacySource::~MongoDBPocoLegacySource() = default;

Chunk MongoDBPocoLegacySource::generate()
{
    if (all_read)
        return {};

    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    for (const auto i : collections::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    while (num_rows < max_block_size)
    {
        auto documents = cursor.nextDocuments(*connection);

        for (auto & document : documents)
        {
            if (document->exists("ok") && document->exists("$err")
                && document->exists("code") && document->getInteger("ok") == 0)
            {
                auto code = document->getInteger("code");
                const Poco::MongoDB::Element::Ptr value = document->get("$err");
                auto message = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(*value).value();
                throw Exception(ErrorCodes::MONGODB_ERROR, "Got error from MongoDB: {}, code: {}", message, code);
            }
            ++num_rows;

            for (const auto idx : collections::range(0, size))
            {
                const auto & name = description.sample_block.getByPosition(idx).name;

                bool exists_in_current_document = document->exists(name);
                if (!exists_in_current_document)
                {
                    insertDefaultValue(*columns[idx], *description.sample_block.getByPosition(idx).column);
                    continue;
                }

                const Poco::MongoDB::Element::Ptr value = document->get(name);

                if (value.isNull() || value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                {
                    insertDefaultValue(*columns[idx], *description.sample_block.getByPosition(idx).column);
                }
                else
                {
                    bool is_nullable = description.types[idx].second;
                    if (is_nullable)
                    {
                        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                        insertValue(column_nullable.getNestedColumn(), description.types[idx].first, *value, name, array_info, idx);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(*columns[idx], description.types[idx].first, *value, name, array_info, idx);
                }
            }
        }

        if (cursor.cursorID() == 0)
        {
            all_read = true;
            break;
        }
    }

    if (num_rows == 0)
        return {};

    return Chunk(std::move(columns), num_rows);
}

}
#endif
