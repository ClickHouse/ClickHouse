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
struct PODArraySmallestJSONStream;

template <typename PODArrayType, FormatStyle format>
using PODArraySmallestJSONStreamPtr = std::unique_ptr<PODArraySmallestJSONStream<PODArrayType, format>>;

template <typename BufferType, FormatStyle format>
struct BufferSmallestJSONStream;

template <typename BufferType, FormatStyle format>
using BufferSmallestJSONStreamPtr = std::unique_ptr<BufferSmallestJSONStream<BufferType, format>>;

struct SmallestJSONStreamFactory
{
    template<FormatStyle format, typename BufferType>
    static BufferSmallestJSONStreamPtr<BufferType, format> fromBuffer(BufferType * buffer, const FormatSettings & settings);

    template <FormatStyle format, typename PODArrayType>
    static PODArraySmallestJSONStreamPtr<PODArrayType, format> fromPODArray(PODArrayType & array, const FormatSettings & settings);
};

}
