#pragma once

#include <Core/Types.h>
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
    template<FormatStyle format, typename BufferType>
    static JSONBStreamBufferPtr<format> fromBuffer(BufferType * buffer, const FormatSettings & settings);

    template<FormatStyle format>
    static JSONBStreamBufferPtr<format> from(BufferBase * buffer, const FormatSettings & settings);
};

}
