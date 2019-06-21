#pragma once

#include <Core/Types.h>
#include <Columns/ColumnString.h>
#include <Formats/FormatSettings.h>

namespace DB
{

enum class RapidFormat
{
    CSV,
    JSON,
    QUOTED,
    ESCAPED,
};

template <typename PODArrayType, RapidFormat format>
struct PODArraySmallestJSONStream;

template <typename PODArrayType, RapidFormat format>
using PODArraySmallestJSONStreamPtr = std::unique_ptr<PODArraySmallestJSONStream<PODArrayType, format>>;

template <typename BufferType, RapidFormat format>
struct BufferSmallestJSONStream;

template <typename BufferType, RapidFormat format>
using BufferSmallestJSONStreamPtr = std::unique_ptr<BufferSmallestJSONStream<BufferType, format>>;

struct SmallestJSONStreamFactory
{
    template<RapidFormat format, typename BufferType>
    static BufferSmallestJSONStreamPtr<BufferType, format> fromBuffer(BufferType * buffer, const FormatSettings & settings);

    template <RapidFormat format, typename PODArrayType>
    static PODArraySmallestJSONStreamPtr<PODArrayType, format> fromPODArray(PODArrayType & array, const FormatSettings & settings);
};

}
