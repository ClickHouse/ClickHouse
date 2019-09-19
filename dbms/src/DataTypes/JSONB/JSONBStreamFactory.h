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

template <typename BufferType, FormatStyle format>
struct JSONBStreamBuffer;

template <typename BufferType, FormatStyle format>
using JSONBStreamBufferPtr = std::unique_ptr<JSONBStreamBuffer<BufferType, format>>;

struct JSONBStreamFactory
{
    template<FormatStyle format, typename BufferType>
    static JSONBStreamBufferPtr<BufferType, format> fromBuffer(BufferType * buffer, const FormatSettings & settings);
};

}
