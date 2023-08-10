#include "MongoDBSource.h"

#include <string>
#include <vector>

#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/ObjectId.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <Poco/URI.h>
#include <Poco/Version.h>

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


std::unique_ptr<Poco::MongoDB::Cursor> createCursor(const std::string & database, const std::string & collection, const Block & sample_block_to_select)
{
    auto cursor = std::make_unique<Poco::MongoDB::Cursor>(database, collection);

    /// Looks like selecting _id column is implicit by default.
    if (!sample_block_to_select.has("_id"))
        cursor->query().returnFieldSelector().add("_id", 0);

    for (const auto & column : sample_block_to_select)
        cursor->query().returnFieldSelector().add(column.name, 1);
    return cursor;
}

MongoDBSource::MongoDBSource(
    std::shared_ptr<Poco::MongoDB::Connection> & connection_,
    std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
    const Block & sample_block,
    UInt64 max_block_size_)
    : ISource(sample_block.cloneEmpty())
    , connection(connection_)
    , cursor{std::move(cursor_)}
    , max_block_size{max_block_size_}
{
    description.init(sample_block);
}


MongoDBSource::~MongoDBSource() = default;


namespace
{
    using ValueType = ExternalResultDescription::ValueType;
    using ObjectId = Poco::MongoDB::ObjectId;

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

    void insertValue(IColumn & column, const ValueType type, const Poco::MongoDB::Element & value, const std::string & name)
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
                else
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch, expected String (UUID), got type id = {} for column {}",
                                        toString(value.type()), name);
                break;
            }
            default:
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Value of unsupported type:{}", column.getName());
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column) { column.insertFrom(sample_column, 0); }
}


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
        Poco::MongoDB::ResponseMessage & response = cursor->next(*connection);

        for (auto & document : response.documents())
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
                        insertValue(column_nullable.getNestedColumn(), description.types[idx].first, *value, name);
                        column_nullable.getNullMapData().emplace_back(0);
                    }
                    else
                        insertValue(*columns[idx], description.types[idx].first, *value, name);
                }
            }
        }

        if (response.cursorID() == 0)
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
