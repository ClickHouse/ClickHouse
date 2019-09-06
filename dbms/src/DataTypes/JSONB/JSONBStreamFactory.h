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

template <typename PODArrayType, FormatStyle format>
struct JSONBStreamPODArray;

template <typename PODArrayType, FormatStyle format>
using JSONBStreamPODArrayPtr = std::unique_ptr<JSONBStreamPODArray<PODArrayType, format>>;

template <typename BufferType, FormatStyle format>
struct JSONBStreamBuffer;

template <typename BufferType, FormatStyle format>
using JSONBStreamBufferPtr = std::unique_ptr<JSONBStreamBuffer<BufferType, format>>;

struct JSONBStreamFactory
{
    template<FormatStyle format, typename BufferType>
    static JSONBStreamBufferPtr<BufferType, format> fromBuffer(BufferType * buffer, const FormatSettings & settings);

    template <FormatStyle format, typename PODArrayType>
    static JSONBStreamPODArrayPtr<PODArrayType, format> fromPODArray(PODArrayType & array, const FormatSettings & settings);
};

}
