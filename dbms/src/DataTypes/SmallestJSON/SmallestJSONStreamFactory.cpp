#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <DataTypes/SmallestJSON/BufferSmallestJSONStream.h>
#include <DataTypes/SmallestJSON/PODArraySmallestJSONStream.h>

namespace DB
{

template<RapidFormat format, typename BufferType>
BufferSmallestJSONStreamPtr<BufferType, format> SmallestJSONStreamFactory::fromBuffer(BufferType * buffer, const DB::FormatSettings & settings)
{
    return std::make_unique<BufferSmallestJSONStream<BufferType, format>>(buffer, settings);
}

template <RapidFormat format, typename PODArrayType>
PODArraySmallestJSONStreamPtr<PODArrayType, format> SmallestJSONStreamFactory::fromPODArray(PODArrayType & array, const FormatSettings & settings)
{
    return std::make_unique<PODArraySmallestJSONStream<PODArrayType, format>>(array, settings);
}

template <RapidFormat format>
using ReadBufferStreamPtr = BufferSmallestJSONStreamPtr<ReadBuffer, format>;

template ReadBufferStreamPtr<RapidFormat::CSV> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<RapidFormat::JSON> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<RapidFormat::QUOTED> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<RapidFormat::ESCAPED> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);

template <RapidFormat format>
using WriteBufferStreamPtr = BufferSmallestJSONStreamPtr<WriteBuffer, format>;

template WriteBufferStreamPtr<RapidFormat::CSV> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<RapidFormat::JSON> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<RapidFormat::QUOTED> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<RapidFormat::ESCAPED> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);

template <RapidFormat format>
using PODArrayWriteStreamPtr = PODArraySmallestJSONStreamPtr<const ColumnString::Chars, format>;

template PODArrayWriteStreamPtr<RapidFormat::CSV> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayWriteStreamPtr<RapidFormat::JSON> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayWriteStreamPtr<RapidFormat::QUOTED> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayWriteStreamPtr<RapidFormat::ESCAPED> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);

//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, RapidFormat::CSV> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, RapidFormat::JSON> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, RapidFormat::QUOTED> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, RapidFormat::ESCAPED> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
}
