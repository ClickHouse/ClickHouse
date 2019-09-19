#pragma once

#include <Core/Types.h>
#include <rapidjson/reader.h>
#include <IO/ReadBuffer.h>
#include <Columns/JSONB/JSONStructAndDataColumn.h>
#include <Columns/ColumnJSONB.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_PARSE_JSON;
}

struct JSONBSerialization
{

    template <typename OutputStream>
    static void serialize(const IColumn & column_, size_t row_num, OutputStream & output_stream);

    template <typename InputStream>
    static void deserialize(IColumn & column_, InputStream & input_stream);

    template <typename OutputStream>
    static void serialize(const IColumn & column_, size_t row_num, const std::unique_ptr<OutputStream> & output_stream)
    {
        OutputStream & output = *output_stream.get();
        serialize(column_, row_num, output);
    }

    template <typename InputStream>
    static void deserialize(IColumn & column_, const std::unique_ptr<InputStream> & input_stream)
    {
        InputStream & input = *input_stream.get();
        deserialize(column_, input);
    }
};

}
