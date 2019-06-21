#include <DataTypes/SmallestJSON/SmallestJSONSerialization.h>
#include <ext/bit_cast.h>
#include <Columns/ColumnString.h>
#include <rapidjson/prettywriter.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/SmallestJSON/BufferSmallestJSONStream.h>
#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <DataTypes/SmallestJSON/PODArraySmallestJSONStream.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_JSON;
}

namespace
{
    const static DataTypePtr & NUMBER_DATA_TYPE = std::make_shared<DataTypeUInt64>();
    const static DataTypePtr & STRING_DATA_TYPE = std::make_shared<DataTypeString>();
    const static DataTypePtr & BOOLEAN_DATA_TYPE = std::make_shared<DataTypeUInt8>();

    inline UInt8 maybeFillingJSONTypeToMark(const ColumnSmallestJSONStructPtr & struct_info)
    {
        return struct_info->data_columns.size() == 1 && !struct_info->children.empty() ? UInt8(TypeIndex::Generics) : UInt8(TypeIndex::Nothing);
    }

    static inline void normalizeForDeserialize(ColumnSmallestJSON & column, size_t old_column_size)
    {
        std::vector<ColumnSmallestJSON::WrappedPtr> & marks = column.getMarks();
        std::vector<ColumnSmallestJSON::WrappedPtr> & fields = column.getFields();

        for (size_t index = 0; index < marks.size(); ++index)
            if (marks[index]->size() == old_column_size)
                marks[index]->insertDefault();

        for (size_t index = 0; index < fields.size(); ++index)
            if (fields[index]->size() == old_column_size)
                fields[index]->insertDefault();
    }
}

struct JSONStructAndColumnBinder
{
    typedef char Ch;

    size_t row_size;
    ColumnSmallestJSONStructPtr parent_struct_info;
    ColumnSmallestJSONStructPtr current_struct_info;

    JSONStructAndColumnBinder(size_t row_size, ColumnSmallestJSONStructPtr & struct_info)
        : row_size(row_size), parent_struct_info(struct_info), current_struct_info(struct_info)
    {
    }

    template <bool check_row_size = false, typename ColumnType>
    bool NumberImpl(IColumn * column, ColumnType number, ColumnType fill_value = ColumnType())
    {
        if (auto * number_column = static_cast<ColumnVector<ColumnType> *>(column))
        {
            typename ColumnVector<ColumnType>::Container & vec_data = number_column->getData();

            if constexpr (check_row_size)
            {
                if (unlikely(vec_data.size() != row_size && vec_data.size()))
                {
                    std::stringstream exception_message;
                    exception_message << "Cannot parse JSON: attribute";

                    for (size_t index = 0; index < current_struct_info->access_path.size(); ++index)
                        exception_message << (index ? "." : " ") << current_struct_info->access_path[index];

                    exception_message << " has duplicate attributes";
                    throw Exception(exception_message.str(), ErrorCodes::CANNOT_PARSE_JSON);
                }
            }

            if (!vec_data.size() && row_size)
                vec_data.resize_fill(row_size, fill_value);

            vec_data.push_back(number);
            return true;
        }
        else
            throw Exception("It is bug.", ErrorCodes::LOGICAL_ERROR);
    }

    bool Null()
    {
        /// TODO:
        throw Exception("Cannot value is null", ErrorCodes::LOGICAL_ERROR);
    }

    bool Bool(bool value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(BOOLEAN_DATA_TYPE);
        return NumberImpl<true, UInt8>(mark_column, UInt8(TypeIndex::UInt8), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, UInt8(value));
    }

    bool Int(Int32 value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Int32), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
    }

    bool Uint(UInt32 value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::UInt32), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
    }

    bool Int64(Int64 value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Int64), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
    }

    bool Uint64(UInt64 value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::UInt64), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, UInt64(value));
    }

    bool Double(Float64 value)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(NUMBER_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::Float64), maybeFillingJSONTypeToMark(current_struct_info)) &&
               NumberImpl(field_column, ext::bit_cast<UInt64>(value));
    }

    bool StringImpl(IColumn * column, const char * data, rapidjson::SizeType length)
    {
        if (ColumnString * column_string = static_cast<ColumnString *>(column))
        {
            ColumnString::Chars & column_string_data = column_string->getChars();
            ColumnString::Offsets & column_offsets_data = column_string->getOffsets();

            if (!column_offsets_data.size() && row_size)
            {
                column_string_data.resize_fill(row_size, 0);
                for (size_t index = 0; index < row_size; ++index)
                    column_offsets_data.push_back(index + 1);
            }

            column_string_data.insert(data, data + length);
            column_string_data.push_back(0);
            column_offsets_data.push_back(column_string_data.size());
            return true;
        }
        else
            throw Exception("It is bug", ErrorCodes::LOGICAL_ERROR);
    }

    bool String(const char * data, rapidjson::SizeType length, bool /*copy*/)
    {
        IColumn * mark_column = current_struct_info->getOrCreateMarkColumn();
        IColumn * field_column = current_struct_info->getOrCreateDataColumn(STRING_DATA_TYPE);

        return NumberImpl<true>(mark_column, UInt8(TypeIndex::String), maybeFillingJSONTypeToMark(current_struct_info)) &&
               StringImpl(field_column, data, length);
    }

    bool Key(const char * data, rapidjson::SizeType length, bool /*copy*/)
    {
        current_struct_info = parent_struct_info->getOrCreateChildren(StringRef(data, length));
        return true;
    }

    bool StartObject()
    {
        parent_struct_info = current_struct_info;

        if (current_struct_info->mark_column)
            NumberImpl<true>(current_struct_info->mark_column, UInt8(TypeIndex::Generics));

        return true;
    }

    bool EndObject(rapidjson::SizeType /*type*/)
    {
        parent_struct_info = parent_struct_info->getParent();
        current_struct_info = current_struct_info->getParent();
        return true;
    }

    bool RawNumber(const Ch * /*ch*/, rapidjson::SizeType /*type*/, bool /*b*/)
    {
        throw Exception("Method RawNumber is not supported for JSONStructAndColumnBinder.", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool StartArray()
    {
        throw Exception("SmallestJSON type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool EndArray(rapidjson::SizeType /*type*/)
    {
        throw Exception("SmallestJSON type does not support parse array json.", ErrorCodes::NOT_IMPLEMENTED);
    }
};


template <typename InputStream>
void SmallestJSONSerialization::deserialize(DB::IColumn & column_, const DB::FormatSettings & settings, InputStream & input_stream)
{
    rapidjson::Reader reader;
    ColumnSmallestJSON & column = static_cast<ColumnSmallestJSON &>(column_);

    if (likely(!settings.input_allow_errors_num && !settings.input_allow_errors_ratio))
    {
        /// In this case, we don't need any rollback operations.
        const size_t old_column_rows = column.size();
        ColumnSmallestJSONStructPtr json_struct = column.getStruct();
        JSONStructAndColumnBinder fields_binder = JSONStructAndColumnBinder(old_column_rows, json_struct);

        input_stream.SkipQuoted();
        if (!reader.Parse<rapidjson::kParseStopWhenDoneFlag>(input_stream, fields_binder))
            throw Exception("Invalid JSON characters.", ErrorCodes::CANNOT_PARSE_JSON);

        input_stream.SkipQuoted();
        normalizeForDeserialize(column, old_column_rows);
    }
    else
    {
        /// In this case, it's always slow.
        MutableColumnPtr temp_column = column.cloneEmpty();
        ColumnSmallestJSON * temp_json_column = static_cast<ColumnSmallestJSON *>(temp_column.get());
        ColumnSmallestJSONStructPtr json_struct = temp_json_column->getStruct();
        JSONStructAndColumnBinder fields_binder = JSONStructAndColumnBinder(temp_column->size(), json_struct);

        input_stream.SkipQuoted();
        if (!reader.Parse<rapidjson::kParseStopWhenDoneFlag>(input_stream, fields_binder))
            throw Exception("Invalid JSON characters.", ErrorCodes::CANNOT_PARSE_JSON);

        input_stream.SkipQuoted();
        column.insertRangeFrom(*temp_json_column, 0, temp_json_column->size());
    }
}


template <TypeIndex type_index, typename ColumnType, typename RapidWrite>
void formatJSONNumber(const IColumn &column, size_t row_num, RapidWrite &writer);

template <typename RapidWrite>
void formatJSONString(const IColumn &column, size_t row_num, RapidWrite &writer);

template <typename RapidWrite>
void formatJSONObject(const ColumnSmallestJSONStructPtr &struct_info, size_t row_num, RapidWrite &writer);

template <typename RapidWrite>
void formatJSON(const ColumnSmallestJSONStructPtr &struct_info, size_t row_num, RapidWrite &writer);

template<typename OutputStream>
void SmallestJSONSerialization::serialize(const IColumn & column, size_t row_num, OutputStream & output_stream)
{
    rapidjson::Writer<OutputStream> writer(output_stream);
    const ColumnSmallestJSON & smallest_json_column = typeid_cast<const ColumnSmallestJSON &>(column);
    formatJSON(smallest_json_column.getStruct(), row_num, writer);
}


template <TypeIndex type_index, typename ColumnType, typename RapidWrite>
void formatJSONNumber(const IColumn & column, size_t row_num, RapidWrite & writer)
{
    const ColumnVector<ColumnType> & data_column = static_cast<const ColumnVector<ColumnType> &>(column);

    const typename ColumnVector<ColumnType>::Container & data = data_column.getData();

    if constexpr (type_index == TypeIndex::UInt8)
        writer.Bool(bool(data[row_num]));
    else if constexpr (type_index == TypeIndex::Float64)
        writer.Double(ext::bit_cast<Float64>(data[row_num]));
    else if constexpr (type_index == TypeIndex::Int32 || type_index == TypeIndex::Int64)
        writer.Int64(ext::bit_cast<Int64>(data[row_num]));
    else if constexpr (type_index == TypeIndex::UInt32 || type_index == TypeIndex::UInt64)
        writer.Uint64(UInt64(data[row_num]));
}

template <typename RapidWrite>
void formatJSONObject(const ColumnSmallestJSONStructPtr &struct_info, size_t row_num, RapidWrite &writer)
{
    writer.StartObject();
    for (const auto & struct_children : struct_info->children)
        formatJSON<RapidWrite>(struct_children, row_num, writer);
    writer.EndObject();
}

template <typename RapidWrite>
void formatJSONString(const IColumn &column, size_t row_num, RapidWrite &writer)
{
    const ColumnString & data_column = static_cast<const ColumnString &>(column);

    const StringRef & data_ref = data_column.getDataAt(row_num);
    writer.String(data_ref.data, rapidjson::SizeType(data_ref.size));
}

template <typename RapidWrite>
void formatJSON(const ColumnSmallestJSONStructPtr & struct_info, size_t row_num, RapidWrite & writer)
{
    if (!struct_info->name.empty())
        writer.Key(struct_info->name.data(), struct_info->name.size());

    const ColumnUInt8 * mark_column = static_cast<const ColumnUInt8 *>(struct_info->mark_column);

    if (!mark_column && !struct_info->children.empty())
        formatJSONObject(struct_info, row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::Generics))
        formatJSONObject(struct_info, row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::UInt8))
        formatJSONNumber<TypeIndex::UInt8, UInt8>(*struct_info->getDataColumn(BOOLEAN_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::Int32))
        formatJSONNumber<TypeIndex::Int32, UInt64>(*struct_info->getDataColumn(NUMBER_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::UInt32))
        formatJSONNumber<TypeIndex::UInt32, UInt64>(*struct_info->getDataColumn(NUMBER_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::Int64))
        formatJSONNumber<TypeIndex::Int64, UInt64>(*struct_info->getDataColumn(NUMBER_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::UInt64))
        formatJSONNumber<TypeIndex::UInt64, UInt64>(*struct_info->getDataColumn(NUMBER_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::Float64))
        formatJSONNumber<TypeIndex::Float64, UInt64>(*struct_info->getDataColumn(NUMBER_DATA_TYPE), row_num, writer);
    else if (mark_column->getData()[row_num] == UInt8(TypeIndex::String))
        formatJSONString(*struct_info->getDataColumn(STRING_DATA_TYPE), row_num, writer);
}

template void SmallestJSONSerialization::serialize<BufferSmallestJSONStream<WriteBuffer, RapidFormat::CSV>>(const IColumn &, size_t, BufferSmallestJSONStream<WriteBuffer, RapidFormat::CSV> &);
template void SmallestJSONSerialization::serialize<BufferSmallestJSONStream<WriteBuffer, RapidFormat::JSON>>(const IColumn &, size_t, BufferSmallestJSONStream<WriteBuffer, RapidFormat::JSON> &);
template void SmallestJSONSerialization::serialize<BufferSmallestJSONStream<WriteBuffer, RapidFormat::QUOTED>>(const IColumn &, size_t, BufferSmallestJSONStream<WriteBuffer, RapidFormat::QUOTED> &);
template void SmallestJSONSerialization::serialize<BufferSmallestJSONStream<WriteBuffer, RapidFormat::ESCAPED>>(const IColumn &, size_t, BufferSmallestJSONStream<WriteBuffer, RapidFormat::ESCAPED> &);

template void SmallestJSONSerialization::deserialize<PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::CSV>>(IColumn &, const FormatSettings &, PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::CSV> &);
template void SmallestJSONSerialization::deserialize<PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::JSON>>(IColumn &, const FormatSettings &, PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::JSON> &);
template void SmallestJSONSerialization::deserialize<PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::QUOTED>>(IColumn &, const FormatSettings &, PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::QUOTED> &);
template void SmallestJSONSerialization::deserialize<PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::ESCAPED>>(IColumn &, const FormatSettings &, PODArraySmallestJSONStream<const ColumnString::Chars, RapidFormat::ESCAPED> &);

template void SmallestJSONSerialization::deserialize<BufferSmallestJSONStream<ReadBuffer, RapidFormat::CSV>>(IColumn &, const FormatSettings &, BufferSmallestJSONStream<ReadBuffer, RapidFormat::CSV> &);
template void SmallestJSONSerialization::deserialize<BufferSmallestJSONStream<ReadBuffer, RapidFormat::JSON>>(IColumn &, const FormatSettings &, BufferSmallestJSONStream<ReadBuffer, RapidFormat::JSON> &);
template void SmallestJSONSerialization::deserialize<BufferSmallestJSONStream<ReadBuffer, RapidFormat::QUOTED>>(IColumn &, const FormatSettings &, BufferSmallestJSONStream<ReadBuffer, RapidFormat::QUOTED> &);
template void SmallestJSONSerialization::deserialize<BufferSmallestJSONStream<ReadBuffer, RapidFormat::ESCAPED>>(IColumn &, const FormatSettings &, BufferSmallestJSONStream<ReadBuffer, RapidFormat::ESCAPED> &);

}
