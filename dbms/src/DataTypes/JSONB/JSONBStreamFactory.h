#pragma once

#include <Core/Types.h>
#include <IO/BufferBase.h>
#include <Columns/ColumnString.h>
#include <Formats/FormatSettings.h>

namespace DB
{

enum class FormatStyle
{
    CSV,
    JSON,
    QUOTED,
    ESCAPED,
};

template <FormatStyle format>
struct JSONBStreamBuffer;

template <FormatStyle format>
using JSONBStreamBufferPtr = std::unique_ptr<JSONBStreamBuffer<format>>;

struct JSONBStreamFactory
{
    template<FormatStyle format>
    static JSONBStreamBufferPtr<format> from(ReadBuffer * buffer, const FormatSettings & settings)
    {
        return std::make_unique<JSONBStreamBuffer<format>>(buffer, settings);
    }

    template<FormatStyle format>
    static JSONBStreamBufferPtr<format> from(WriteBuffer * buffer, const FormatSettings & settings)
    {
        return std::make_unique<JSONBStreamBuffer<format>>(buffer, settings);
    }
};

}
