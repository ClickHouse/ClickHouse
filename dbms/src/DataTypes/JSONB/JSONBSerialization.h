#pragma once

#include <Core/Types.h>
#include <rapidjson/reader.h>
#include <IO/ReadBuffer.h>
#include <Columns/JSONB/JSONBDataMark.h>
#include <Columns/JSONB/JSONStructAndDataColumn.h>
#include <Columns/ColumnJSONB.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_JSON;
}

struct JSONBSerialization
{
public:
    template <typename InputStream>
    static void deserialize(IColumn & column_, const std::unique_ptr<InputStream> & input_stream)
    {
        InputStream & input = *input_stream.get();
        deserialize(column_, input);
    }

    template <typename OutputStream>
    static void serialize(const IColumn & column_, size_t row_num, const std::unique_ptr<OutputStream> & output_stream)
    {
        OutputStream & output = *output_stream.get();
        serialize(column_, row_num, output);
    }

private:
    template <typename InputStream>
    static void deserialize(IColumn & column, InputStream & input_stream);

    template <typename OutputStream>
    static void serialize(const IColumn & column_, size_t row_num, OutputStream & output_stream);

    template <typename RapidWrite>
    static void toJSONString(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        const auto * mark_column = static_cast<const ColumnUInt8 *>(struct_info->mark_column);

        if (!isJSONType<JSONBDataMark::Nothing>(mark_column, row_num))
        {
            if (!struct_info->name.empty())
                writer.Key(struct_info->name.data(), struct_info->name.size());

            if (isJSONObject(mark_column, struct_info, row_num))
                toJSONStringWithObject(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::String>(mark_column, writer))
                toJSONStringWithString(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::Bool>(mark_column, row_num))
                toJSONStringWithBoolean(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::Int64>(mark_column, row_num))
                toJSONStringWithNumber<Int64>(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::UInt64>(mark_column, row_num))
                toJSONStringWithNumber<UInt64>(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::Float64>(mark_column, row_num))
                toJSONStringWithNumber<Float64>(struct_info, row_num, writer);
            else if (isJSONType<JSONBDataMark::BinaryJSON>(struct_info, row_num, writer))
                toJSONStringWithBinaryJSON(struct_info, row_num, writer);
        }
    }

    template <JSONBDataMark check_type>
    static inline bool isJSONType(const ColumnUInt8 *mark_column, size_t row_num)
    {
        return mark_column && mark_column->getData()[row_num] == UInt8(check_type);
    }

    static inline bool isJSONObject(const ColumnUInt8 * mark_column, const ColumnJSONBStructPtr & column_struct, size_t row_num)
    {
        return (!mark_column && !column_struct->children.empty()) ||
               (mark_column && mark_column->getData()[row_num] == UInt8(JSONBDataMark::Object));
    }

    template <typename RapidWrite>
    static inline void toJSONStringWithObject(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        writer.StartObject();
        for (const auto & struct_children : struct_info->children)
            toJSONString<RapidWrite>(struct_children, row_num, writer);
        writer.EndObject();
    }

    template <typename RapidWrite>
    static inline void toJSONStringWithNumber(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        const auto & data_column = JSONBDataMarkType::getDataMarkType<JSONBDataMark::Bool, ColumnUInt8>(struct_info);
        writer.Bool(bool(data_column->getData()[row_num]));
    }

    template <typename RapidWrite>
    static inline void toJSONStringWithBoolean(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        const auto & data_column = JSONBDataMarkType::getDataMarkType<JSONBDataMark::Bool, ColumnUInt8>(struct_info);
        writer.Bool(bool(data_column->getData()[row_num]));
    }

    template <typename RapidWrite>
    static inline void toJSONStringWithString(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        const auto & data_column = JSONBDataMarkType::getDataMarkType<JSONBDataMark::String, ColumnString>(struct_info);

        const StringRef & data_ref = data_column->getDataAt(row_num);
        writer.String(data_ref.data, rapidjson::SizeType(data_ref.size));
    }

    template <typename RapidWrite>
    static inline void toJSONStringWithBinaryJSON(const ColumnJSONBStructPtr & struct_info, size_t row_num, RapidWrite & writer)
    {
        const auto & data_column = JSONBDataMarkType::getDataMarkType<JSONBDataMark::BinaryJSON, ColumnString>(struct_info);

        const StringRef & data_ref = data_column->getDataAt(row_num);
        writer.String(data_ref.data, rapidjson::SizeType(data_ref.size));
    }
};

}
