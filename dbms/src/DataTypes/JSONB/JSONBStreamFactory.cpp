#include <DataTypes/JSONB/JSONBStreamFactory.h>
#include <DataTypes/JSONB/JSONBStreamBuffer.h>
#include <DataTypes/JSONB/JSONBStreamPODArray.h>

namespace DB
{

template<FormatStyle format, typename BufferType>
JSONBStreamBufferPtr<BufferType, format> JSONBStreamFactory::fromBuffer(BufferType * buffer, const DB::FormatSettings & settings)
{
    return std::make_unique<JSONBStreamBuffer<BufferType, format>>(buffer, settings);
}

template <FormatStyle format, typename PODArrayType>
JSONBStreamPODArrayPtr<PODArrayType, format> JSONBStreamFactory::fromPODArray(PODArrayType & array, const FormatSettings & settings)
{
    return std::make_unique<JSONBStreamPODArray<PODArrayType, format>>(array, settings);
}

template <FormatStyle format>
using ReadBufferStreamPtr = JSONBStreamBufferPtr<ReadBuffer, format>;

template ReadBufferStreamPtr<FormatStyle::CSV> JSONBStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::JSON> JSONBStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::QUOTED> JSONBStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);
template ReadBufferStreamPtr<FormatStyle::ESCAPED> JSONBStreamFactory::fromBuffer(ReadBuffer *, const DB::FormatSettings &);

template <FormatStyle format>
using WriteBufferStreamPtr = JSONBStreamBufferPtr<WriteBuffer, format>;

template WriteBufferStreamPtr<FormatStyle::CSV> JSONBStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::JSON> JSONBStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::QUOTED> JSONBStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);
template WriteBufferStreamPtr<FormatStyle::ESCAPED> JSONBStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);

template <FormatStyle format>
using PODArrayReadStreamPtr = JSONBStreamPODArrayPtr<const ColumnString::Chars, format>;

template PODArrayReadStreamPtr<FormatStyle::CSV> JSONBStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::JSON> JSONBStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::QUOTED> JSONBStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);
template PODArrayReadStreamPtr<FormatStyle::ESCAPED> JSONBStreamFactory::fromPODArray(const ColumnString::Chars &, const DB::FormatSettings &);

//template JSONBStreamPODArrayPtr<ColumnString::Chars, FormatStyle::CSV> JSONBStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template JSONBStreamPODArrayPtr<ColumnString::Chars, FormatStyle::JSON> JSONBStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template JSONBStreamPODArrayPtr<ColumnString::Chars, FormatStyle::QUOTED> JSONBStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
//template JSONBStreamPODArrayPtr<ColumnString::Chars, FormatStyle::ESCAPED> JSONBStreamFactory::fromPODArray(ColumnString::Chars &, const DB::FormatSettings &);
}
