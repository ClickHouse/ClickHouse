#include <Processors/Sources/MongoDBSource.h>

#include <string>
#include <vector>

#include <Poco/MongoDB/Array.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/OpMsgCursor.h>
#include <Poco/MongoDB/ObjectId.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <Poco/URI.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

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
}

namespace
{
    using ValueType = ExternalResultDescription::ValueType;
    using ObjectId = Poco::MongoDB::ObjectId;
    using MongoArray = Poco::MongoDB::Array;
    using MongoDocument = Poco::MongoDB::Document;

    template <typename T>
    Field getNumberField(const Poco::MongoDB::Element & value, const std::string & name)
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

    Field getValueField(
        const DataTypePtr & input_data_type,
        const Poco::MongoDB::Element & value,
        const std::string & name)
    {
        if (value.type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
            return input_data_type->getDefault();

        auto [type, _] = ExternalResultDescription::getValueTypeWithNullable(input_data_type);
        auto data_type = removeNullable(input_data_type);

        switch (type)
        {
            case ValueType::vtUInt8:
                return getNumberField<UInt8>(value, name);
            case ValueType::vtUInt16:
                return getNumberField<UInt16>(value, name);
            case ValueType::vtUInt32:
                return getNumberField<UInt32>(value, name);
            case ValueType::vtUInt64:
                return getNumberField<UInt64>(value, name);
            case ValueType::vtInt8:
                return getNumberField<Int8>(value, name);
            case ValueType::vtInt16:
                return getNumberField<Int16>(value, name);
            case ValueType::vtInt32:
                return getNumberField<Int32>(value, name);
            case ValueType::vtInt64:
                return getNumberField<Int64>(value, name);
            case ValueType::vtFloat32:
                return getNumberField<Float32>(value, name);
            case ValueType::vtFloat64:
                return getNumberField<Float64>(value, name);

            case ValueType::vtString:
            case ValueType::vtFixedString:
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
            }
            case ValueType::vtDate:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                return static_cast<UInt16>(DateLUT::instance().toDayNum(
                    static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime()));
            }
            case ValueType::vtDateTime:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Timestamp, got type id = {} for column {}",
                                    toString(value.type()), name);

                return static_cast<UInt32>(static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime());
            }
            case ValueType::vtUUID:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<String>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String (UUID), got type id = {} for column {}",
                                        toString(value.type()), name);

                String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                return parse<UUID>(string);
            }
            case ValueType::vtArray:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<MongoArray::Ptr>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Array, got type id = {} for column {}",
                                    toString(value.type()), name);

                const auto nested_type = assert_cast<const DataTypeArray *>(data_type.get())->getNestedType();
                auto parent = static_cast<const Poco::MongoDB::ConcreteElement<MongoArray::Ptr> &>(value).value();
                const size_t size = parent->size();

                Field field = Array();
                Array & arr = field.get<Array &>();
                arr.reserve(size);

                for (size_t child_idx = 0; child_idx < size; ++child_idx)
                {
                    Poco::MongoDB::Element::Ptr child = parent->get(static_cast<int>(child_idx));
                    arr.push_back(getValueField(nested_type, *child, name));
                }
                return field;
            }
            case ValueType::vtTuple:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<MongoDocument::Ptr>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Tuple, got type id = {} for column {}",
                                    toString(value.type()), name);

                auto document = static_cast<const Poco::MongoDB::ConcreteElement<MongoDocument::Ptr> &>(value).value();

                const auto * tuple_type = assert_cast<const DataTypeTuple *>(data_type.get());
                const Strings & nested_names = tuple_type->getElementNames();
                const DataTypes & nested_types = tuple_type->getElements();

                const size_t size = nested_names.size();

                Field field = Tuple();
                Tuple & tuple = field.get<Tuple &>();
                tuple.reserve(size);

                for (size_t i = 0; i < size; ++i)
                {
                    const auto & nested_name = nested_names[i];
                    const auto & nested_type = nested_types[i];

                    if (!document->exists(nested_name))
                    {
                        tuple.push_back(nested_type->getDefault());
                        continue;
                    }

                    const Poco::MongoDB::Element::Ptr nested_value = document->get(nested_name);

                    if (nested_value.isNull() || nested_value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                    {
                        tuple.push_back(nested_type->getDefault());
                        continue;
                    }

                    tuple.push_back(getValueField(nested_type, *nested_value, name));
                }

                return field;
            }
            default:
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Value of unsupported type: {}", data_type->getName());
        }

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

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }

    void insertValue(
        IColumn & column,
        const IDataType & data_type,
        const ValueType type,
        const Poco::MongoDB::Element & value,
        const std::string & name,
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
                else
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String (UUID), got type id = {} for column {}",
                                        toString(value.type()), name);
                break;
            }
            case ValueType::vtArray:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<MongoArray::Ptr>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Array, got type id = {} for column {}",
                                    toString(value.type()), name);

                const auto nested_type = assert_cast<const DataTypeArray &>(data_type).getNestedType();
                auto parent = static_cast<const Poco::MongoDB::ConcreteElement<MongoArray::Ptr> &>(value).value();
                const size_t size = parent->size();

                Field field = Array();
                Array & arr = field.get<Array &>();
                arr.reserve(size);

                for (size_t child_idx = 0; child_idx < size; ++child_idx)
                {
                    Poco::MongoDB::Element::Ptr child = parent->get(static_cast<int>(child_idx));
                    arr.push_back(getValueField(nested_type, *child, name));
                }

                assert_cast<ColumnArray &>(column).insert(field);
                break;
            }
            case ValueType::vtTuple:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<MongoDocument::Ptr>::TypeId)
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected Tuple, got type id = {} for column {}",
                                    toString(value.type()), name);

                auto document = static_cast<const Poco::MongoDB::ConcreteElement<MongoDocument::Ptr> &>(value).value();

                ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(column);

                const auto * tuple_type = assert_cast<const DataTypeTuple *>(&data_type);
                const Strings & names = tuple_type->getElementNames();
                const DataTypes & nested_types = tuple_type->getElements();

                for (const auto col_num : collections::range(0, column_tuple.tupleSize()))
                {
                    IColumn & nested_column = column_tuple.getColumn(col_num);
                    const DataTypePtr & nested_type = nested_types[col_num];

                    const auto & nested_name = names[col_num];
                    auto nested_external_desc = ExternalResultDescription::getValueTypeWithNullable(nested_type);

                    bool exists_in_current_document = document->exists(nested_name);
                    if (!exists_in_current_document)
                    {
                        nested_column.insertDefault();
                        continue;
                    }

                    const Poco::MongoDB::Element::Ptr nested_value = document->get(nested_name);

                    if (nested_value.isNull() || nested_value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                    {
                        nested_column.insertDefault();
                        continue;
                    }

                    bool is_nullable = nested_external_desc.second;
                    if (is_nullable)
                    {
                        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(nested_column);
                        insertValue(column_nullable.getNestedColumn(), *nested_type, nested_external_desc.first, *nested_value, nested_name, idx);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(nested_column, *nested_type, nested_external_desc.first, *nested_value, nested_name, idx);
                }

                break;
            }
            default:
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Value of unsupported type: {}", column.getName());
        }
    }
}


bool isMongoDBWireProtocolOld(Poco::MongoDB::Connection & connection_)
{
    Poco::MongoDB::Database db("config");
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


MongoDBCursor::MongoDBCursor(
    const std::string & database,
    const std::string & collection,
    const Block & sample_block_to_select,
    const Poco::MongoDB::Document & query,
    Poco::MongoDB::Connection & connection)
    : is_wire_protocol_old(isMongoDBWireProtocolOld(connection))
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
        if (sample_block_to_select)
            old_cursor->query().returnFieldSelector() = projection;
    }
    else
    {
        new_cursor = std::make_unique<Poco::MongoDB::OpMsgCursor>(database, collection);
        new_cursor->query().setCommandName(Poco::MongoDB::OpMsgMessage::CMD_FIND);
        new_cursor->query().body().addNewDocument("filter") = query;
        if (sample_block_to_select)
            new_cursor->query().body().addNewDocument("projection") = projection;
    }
}

Poco::MongoDB::Document::Vector MongoDBCursor::nextDocuments(Poco::MongoDB::Connection & connection)
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

Int64 MongoDBCursor::cursorID() const
{
    return cursor_id;
}


MongoDBSource::MongoDBSource(
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
}


MongoDBSource::~MongoDBSource() = default;

Chunk MongoDBSource::generate()
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
                const auto & type = description.sample_block.getByPosition(idx).type;
                const auto & sample_column = description.sample_block.getByPosition(idx).column;

                bool exists_in_current_document = document->exists(name);
                if (!exists_in_current_document)
                {
                    insertDefaultValue(*columns[idx], *sample_column);
                    continue;
                }

                const Poco::MongoDB::Element::Ptr value = document->get(name);

                if (value.isNull() || value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                {
                    insertDefaultValue(*columns[idx], *sample_column);
                }
                else
                {
                    bool is_nullable = description.types[idx].second;
                    if (is_nullable)
                    {
                        ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                        insertValue(column_nullable.getNestedColumn(), *type, description.types[idx].first, *value, name, idx);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(*columns[idx], *type, description.types[idx].first, *value, name, idx);
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
