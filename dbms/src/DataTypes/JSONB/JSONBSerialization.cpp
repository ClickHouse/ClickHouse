#include <DataTypes/JSONB/JSONBSerialization.h>
#include <ext/bit_cast.h>
#include <rapidjson/prettywriter.h>
#include <Columns/ColumnString.h>
#include <Columns/JSONB/JSONBDataMark.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/JSONB/JSONBDataBinder.h>
#include <DataTypes/JSONB/JSONBStreamBuffer.h>
#include <DataTypes/JSONB/JSONBStreamFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_JSON;
}

namespace
{
    const static DataTypePtr & BINARY_JSON_TYPE = std::make_shared<DataTypeString>();
    const static DataTypePtr & NUMBER_DATA_TYPE = std::make_shared<DataTypeUInt64>();
    const static DataTypePtr & STRING_DATA_TYPE = std::make_shared<DataTypeString>();
    const static DataTypePtr & BOOLEAN_DATA_TYPE = std::make_shared<DataTypeUInt8>();

//    inline UInt8 maybeFillingJSONTypeToMark(const ColumnJSONBStructPtr & struct_info)
//    {
//        return struct_info->data_columns.size() == 1 && !struct_info->children.empty() ? UInt8(TypeIndex::JSONB) : UInt8(TypeIndex::Nothing);
//    }
//
//    static inline void normalizeForDeserialize(ColumnJSONB & column, size_t old_column_size)
//    {
//        std::vector<ColumnJSONB::WrappedPtr> & marks = column.getMarks();
//        std::vector<ColumnJSONB::WrappedPtr> & fields = column.getFields();
//
//        for (size_t index = 0; index < marks.size(); ++index)
//            if (marks[index]->size() == old_column_size)
//                marks[index]->insertDefault();
//
//        for (size_t index = 0; index < fields.size(); ++index)
//            if (fields[index]->size() == old_column_size)
//                fields[index]->insertDefault();
//    }
}

static inline void storeBinaryJSON(IColumn & column_, const fleece::slice & binary)
{
    ColumnJSONB & column = static_cast<ColumnJSONB &>(column_);

//    const auto & column_struct = column.getStruct();
//    auto & binary_children_column = static_cast<ColumnString &>(*column_struct->getOrCreateDataColumn(BINARY_JSON_TYPE));
//
//    ColumnString::Chars & data = binary_children_column.getChars();
//    ColumnString::Offsets & offsets = binary_children_column.getOffsets();
//    size_t old_chars_size = data.size();
//    size_t old_offsets_size = offsets.size();
//    try
//    {
//        data.insert(reinterpret_cast<const char *>(binary.buf), reinterpret_cast<const char *>(binary.buf) + binary.size);
//        data.push_back(0);
//        offsets.push_back(data.size());
//        static_cast<ColumnUInt8 *>(column_struct->getOrCreateMarkColumn())->insertValue(UInt8(JSONBDataMark::BinaryJSON));
//    }
//    catch (...)
//    {
//        offsets.resize_assume_reserved(old_offsets_size);
//        data.resize_assume_reserved(old_chars_size);
//        throw;
//    }
}

template <typename InputStream>
void JSONBSerialization::deserialize(IColumn & column, InputStream & input_stream)
{
    rapidjson::Reader reader;
    JSONBDataBinder data_binder;

    input_stream.SkipQuoted();
    if (!reader.Parse<rapidjson::kParseStopWhenDoneFlag>(input_stream, data_binder))
        throw Exception("Invalid JSON characters.", ErrorCodes::CANNOT_PARSE_JSON);
    input_stream.SkipQuoted();
    storeBinaryJSON(column, data_binder.finalize());
}

template<typename OutputStream>
void JSONBSerialization::serialize(const IColumn & column, size_t row_num, OutputStream & output_stream)
{
    rapidjson::Writer<OutputStream> writer(output_stream);
    const ColumnJSONB & serialization_json_column = typeid_cast<const ColumnJSONB &>(column);
    toJSONString<rapidjson::Writer<OutputStream>>(serialization_json_column.getStruct(), row_num, writer);
}

template void JSONBSerialization::deserialize<JSONBStreamBuffer<FormatStyle::CSV>>(IColumn &, JSONBStreamBuffer<FormatStyle::CSV> &);
template void JSONBSerialization::deserialize<JSONBStreamBuffer<FormatStyle::JSON>>(IColumn &, JSONBStreamBuffer<FormatStyle::JSON> &);
template void JSONBSerialization::deserialize<JSONBStreamBuffer<FormatStyle::QUOTED>>(IColumn &, JSONBStreamBuffer<FormatStyle::QUOTED> &);
template void JSONBSerialization::deserialize<JSONBStreamBuffer<FormatStyle::ESCAPED>>(IColumn &, JSONBStreamBuffer<FormatStyle::ESCAPED> &);

template void JSONBSerialization::serialize<JSONBStreamBuffer<FormatStyle::CSV>>(const IColumn &, size_t, JSONBStreamBuffer<FormatStyle::CSV> &);
template void JSONBSerialization::serialize<JSONBStreamBuffer<FormatStyle::JSON>>(const IColumn &, size_t, JSONBStreamBuffer<FormatStyle::JSON> &);
template void JSONBSerialization::serialize<JSONBStreamBuffer<FormatStyle::QUOTED>>(const IColumn &, size_t, JSONBStreamBuffer<FormatStyle::QUOTED> &);
template void JSONBSerialization::serialize<JSONBStreamBuffer<FormatStyle::ESCAPED>>(const IColumn &, size_t, JSONBStreamBuffer<FormatStyle::ESCAPED> &);


//struct JSONStructAndColumnBinder
//{
//    typedef char Ch;
//
//    size_t row_size;
//    ColumnJSONBStructPtr parent_struct_info;
//    ColumnJSONBStructPtr current_struct_info;
//
//    JSONStructAndColumnBinder(size_t row_size, ColumnJSONBStructPtr & struct_info)
//        : row_size(row_size), parent_struct_info(struct_info), current_struct_info(struct_info)
//    {
//    }
//
//    template <bool check_row_size = false, typename ColumnType>
//    bool NumberImpl(IColumn * column, ColumnType number, ColumnType fill_value = ColumnType())
//    {
//        if (auto * number_column = static_cast<ColumnVector<ColumnType> *>(column))
//        {
//            typename ColumnVector<ColumnType>::Container & vec_data = number_column->getData();
//
//            if constexpr (check_row_size)
//            {
//                if (unlikely(vec_data.size() != row_size && vec_data.size()))
//                {
//                    std::stringstream exception_message;
//                    exception_message << "Cannot parse JSON: attribute";
//
//                    for (size_t index = 0; index < current_struct_info->access_path.size(); ++index)
//                        exception_message << (index ? "." : " ") << current_struct_info->access_path[index];
//
//                    exception_message << " has duplicate attributes";
//                    throw Exception(exception_message.str(), ErrorCodes::CANNOT_PARSE_JSON);
//                }
//            }
//
//            if (!vec_data.size() && row_size)
//                vec_data.resize_fill(row_size, fill_value);
//
//            vec_data.push_back(number);
//            return true;
//        }
//        else
//            throw Exception("It is bug.", ErrorCodes::LOGICAL_ERROR);
//    }
//
//    bool Null()
//    {
//        /// TODO:
//        throw Exception("Cannot value is null", ErrorCodes::LOGICAL_ERROR);
//    }
//
//    bool Bool(bool value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(BOOLEAN_DATA_TYPE);
//        return NumberImpl<true, UInt8>(mark_column, UInt8(TypeIndex::UInt8), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, UInt8(value));
//    }
//
//    bool Int(Int32 value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Int32), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
//    }
//
//    bool Uint(UInt32 value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::UInt32), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
//    }
//
//    bool Int64(Int64 value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Int64), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
//    }
//
//    bool Uint64(UInt64 value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::UInt64), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, UInt64(value));
//    }
//
//    bool Double(Float64 value)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Float64), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
//    }
//
//    bool StringImpl(IColumn * column, const char * data, rapidjson::SizeType length)
//    {
//        if (ColumnString * column_string = static_cast<ColumnString *>(column))
//        {
//            ColumnString::Chars & column_string_data = column_string->getChars();
//            ColumnString::Offsets & column_offsets_data = column_string->getOffsets();
//
//            if (!column_offsets_data.size() && row_size)
//            {
//                column_string_data.resize_fill(row_size, 0);
//                for (size_t index = 0; index < row_size; ++index)
//                    column_offsets_data.push_back(index + 1);
//            }
//
//            column_string_data.insert(data, data + length);
//            column_string_data.push_back(0);
//            column_offsets_data.push_back(column_string_data.size());
//            return true;
//        }
//        else
//            throw Exception("It is bug", ErrorCodes::LOGICAL_ERROR);
//    }
//
//    bool String(const char * data, rapidjson::SizeType length, bool /*copy*/)
//    {
//        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
//        IColumn * field_column = current_struct_info->getOrCreateDataColumn(STRING_DATA_TYPE);
//
//        return NumberImpl<true>(mark_column, UInt8(TypeIndex::String), maybeFillingJSONTypeToMark(current_struct_info)) &&
//               StringImpl(field_column, data, length);
//    }
//
//    bool Key(const char * data, rapidjson::SizeType length, bool /*copy*/)
//    {
//        current_struct_info = parent_struct_info->getOrCreateChildren(StringRef(data, length));
//        return true;
//    }
//
//    bool StartObject()
//    {
//        parent_struct_info = current_struct_info;
//
//        if (current_struct_info->mark_column)
//            NumberImpl<true>(current_struct_info->mark_column, UInt8(TypeIndex::JSONB));
//
//        return true;
//    }
//
//    bool EndObject(rapidjson::SizeType /*type*/)
//    {
//        parent_struct_info = parent_struct_info->getParent();
//        current_struct_info = current_struct_info->getParent();
//        return true;
//    }
//
//    bool RawNumber(const Ch * /*ch*/, rapidjson::SizeType /*type*/, bool /*b*/)
//    {
//        throw Exception("Method RawNumber is not supported for JSONStructAndColumnBinder.", ErrorCodes::NOT_IMPLEMENTED);
//    }
//
//    bool StartArray()
//    {
//        throw Exception("JSONB type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED);
//    }
//
//    bool EndArray(rapidjson::SizeType /*type*/)
//    {
//        throw Exception("JSONB type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED);
//    }
//};

}
