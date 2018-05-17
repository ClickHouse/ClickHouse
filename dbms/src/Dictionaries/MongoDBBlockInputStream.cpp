#include <Common/config.h>
#if USE_POCO_MONGODB

#include <vector>
#include <string>
#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <Poco/MongoDB/Connection.h>
    #include <Poco/MongoDB/Cursor.h>
    #include <Poco/MongoDB/Element.h>
    #include <Poco/MongoDB/ObjectId.h>
#pragma GCC diagnostic pop

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/MongoDBBlockInputStream.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <ext/range.h>
#include <Common/FieldVisitors.h>


namespace DB
{

MongoDBBlockInputStream::MongoDBBlockInputStream(
    std::shared_ptr<Poco::MongoDB::Connection> & connection_,
    std::unique_ptr<Poco::MongoDB::Cursor> cursor_,
    const Block & sample_block,
    const size_t max_block_size)
    : connection(connection_), cursor{std::move(cursor_)}, max_block_size{max_block_size}
{
    description.init(sample_block);
}


MongoDBBlockInputStream::~MongoDBBlockInputStream() = default;


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
                static_cast<ColumnVector<T> &>(column).getData().push_back(static_cast<const Poco::MongoDB::ConcreteElement<Int32> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<Int64>::TypeId:
                static_cast<ColumnVector<T> &>(column).getData().push_back(static_cast<const Poco::MongoDB::ConcreteElement<Int64> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<Float64>::TypeId:
                static_cast<ColumnVector<T> &>(column).getData().push_back(static_cast<const Poco::MongoDB::ConcreteElement<Float64> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<bool>::TypeId:
                static_cast<ColumnVector<T> &>(column).getData().push_back(static_cast<const Poco::MongoDB::ConcreteElement<bool> &>(value).value());
                break;
            case Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId:
                static_cast<ColumnVector<T> &>(column).getData().emplace_back();
                break;
            case Poco::MongoDB::ElementTraits<String>::TypeId:
                static_cast<ColumnVector<T> &>(column).getData().push_back(
                    parse<T>(static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value()));
                break;
            default:
                throw Exception("Type mismatch, expected a number, got type id = " + toString(value.type()) +
                    " for column " + name, ErrorCodes::TYPE_MISMATCH);
        }
    }

    void insertValue(
        IColumn & column, const ValueType type, const Poco::MongoDB::Element & value, const std::string & name)
    {
        switch (type)
        {
            case ValueType::UInt8: insertNumber<UInt8>(column, value, name); break;
            case ValueType::UInt16: insertNumber<UInt16>(column, value, name); break;
            case ValueType::UInt32: insertNumber<UInt32>(column, value, name); break;
            case ValueType::UInt64: insertNumber<UInt64>(column, value, name); break;
            case ValueType::Int8: insertNumber<Int8>(column, value, name); break;
            case ValueType::Int16: insertNumber<Int16>(column, value, name); break;
            case ValueType::Int32: insertNumber<Int32>(column, value, name); break;
            case ValueType::Int64: insertNumber<Int64>(column, value, name); break;
            case ValueType::Float32: insertNumber<Float32>(column, value, name); break;
            case ValueType::Float64: insertNumber<Float64>(column, value, name); break;

            case ValueType::String:
            {
                if (value.type() == Poco::MongoDB::ElementTraits<ObjectId::Ptr>::TypeId)
                {
                    std::string string_id = value.toString();
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(string_id.data(), string_id.size() + 1);
                    break;
                }
                else if (value.type() == Poco::MongoDB::ElementTraits<String>::TypeId)
                {
                    String string = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(value).value();
                    static_cast<ColumnString &>(column).insertDataWithTerminatingZero(string.data(), string.size() + 1);
                    break;
                }

                throw Exception{"Type mismatch, expected String, got type id = " + toString(value.type()) +
                    " for column " + name, ErrorCodes::TYPE_MISMATCH};
            }

            case ValueType::Date:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception{"Type mismatch, expected Timestamp, got type id = " + toString(value.type()) +
                        " for column " + name, ErrorCodes::TYPE_MISMATCH};

                static_cast<ColumnUInt16 &>(column).getData().push_back(
                    UInt16{DateLUT::instance().toDayNum(
                        static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime())});
                break;
            }

            case ValueType::DateTime:
            {
                if (value.type() != Poco::MongoDB::ElementTraits<Poco::Timestamp>::TypeId)
                    throw Exception{"Type mismatch, expected Timestamp, got type id = " + toString(value.type()) +
                        " for column " + name, ErrorCodes::TYPE_MISMATCH};

                static_cast<ColumnUInt32 &>(column).getData().push_back(
                    static_cast<const Poco::MongoDB::ConcreteElement<Poco::Timestamp> &>(value).value().epochTime());
                break;
            }
        }
    }

    void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }
}


Block MongoDBBlockInputStream::readImpl()
{
    if (all_read)
        return {};

    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    for (const auto i : ext::range(0, size))
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t num_rows = 0;
    while (num_rows < max_block_size)
    {
        Poco::MongoDB::ResponseMessage & response = cursor->next(*connection);

        for (const auto & document : response.documents())
        {
            ++num_rows;

            for (const auto idx : ext::range(0, size))
            {
                const auto & name = description.names[idx];
                const Poco::MongoDB::Element::Ptr value = document->get(name);

                if (value.isNull() || value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
                    insertDefaultValue(*columns[idx], *description.sample_columns[idx]);
                else
                    insertValue(*columns[idx], description.types[idx], *value, name);
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

    return description.sample_block.cloneWithColumns(std::move(columns));
}

}

#endif
