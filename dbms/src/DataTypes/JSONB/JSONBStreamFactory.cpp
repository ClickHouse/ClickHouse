#include <DataTypes/JSONB/JSONBStreamFactory.h>
#include <DataTypes/JSONB/JSONBStreamBuffer.h>

namespace DB
{

template<FormatStyle format, typename BufferType>
JSONBStreamBufferPtr<BufferType, format> JSONBStreamFactory::fromBuffer(BufferType * buffer, const DB::FormatSettings & settings)
{
    return std::make_unique<JSONBStreamBuffer<BufferType, format>>(buffer, settings);
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
template WriteBufferStreamPtr<FormatStyle::ESCAPED> JSONBStreamFactory::fromBuffer(WriteBuffer *, const DB::FormatSettings &);;
}
