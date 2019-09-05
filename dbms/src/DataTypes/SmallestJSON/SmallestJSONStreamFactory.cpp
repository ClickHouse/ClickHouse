#include <DataTypes/SmallestJSON/SmallestJSONStreamFactory.h>
#include <DataTypes/SmallestJSON/BufferSmallestJSONStream.h>
#include <DataTypes/SmallestJSON/PODArraySmallestJSONStream.h>

namespace DB
{

template<FormatStyle format, typename BufferType>
BufferSmallestJSONStreamPtr<BufferType, format> SmallestJSONStreamFactory::fromBuffer(BufferType * buffer, const DB::FormatSettings & settings)
{
    return std::make_unique<BufferSmallestJSONStream<BufferType, format>>(buffer, settings);
}

template <FormatStyle format, typename PODArrayType>
PODArraySmallestJSONStreamPtr<PODArrayType, format> SmallestJSONStreamFactory::fromPODArray(PODArrayType & array, const FormatSettings & settings)
{
    return std::make_unique<PODArraySmallestJSONStream<PODArrayType, format>>(array, settings);
}

template <FormatStyle format>
using ReadBufferStreamPtr = BufferSmallestJSONStreamPtr<ReadBuffer, format>;

template ReadBufferStreamPtr<FormatStyle::CSV> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::JSON> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::QUOTED> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::ESCAPED> SmallestJSONStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);

template <FormatStyle format>
using WriteBufferStreamPtr = BufferSmallestJSONStreamPtr<WriteBuffer, format>;

template WriteBufferStreamPtr<FormatStyle::CSV> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::JSON> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::QUOTED> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::ESCAPED> SmallestJSONStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);

template <FormatStyle format>
using PODArrayReadStreamPtr = PODArraySmallestJSONStreamPtr<const ColumnString::Chars, format>;

template PODArrayReadStreamPtr<FormatStyle::CSV> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::JSON> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::QUOTED> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::ESCAPED> SmallestJSONStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);

//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, FormatStyle::CSV> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, FormatStyle::JSON> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, FormatStyle::QUOTED> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template PODArraySmallestJSONStreamPtr<ColumnString::Chars, FormatStyle::ESCAPED> SmallestJSONStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
}
