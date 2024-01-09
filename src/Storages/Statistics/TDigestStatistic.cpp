#include <Storages/Statistics/TDigestStatistic.h>

namespace DB
{

Float64 TDigestStatistic::estimateLess(Float64 val) const
{
    return data.getCountLessThan(val);
}

void TDigestStatistic::serialize(WriteBuffer & buf)
{
    data.serialize(buf);
}

void TDigestStatistic::deserialize(ReadBuffer & buf)
{
    data.deserialize(buf);
}

void TDigestStatistic::update(const ColumnPtr & column)
{
    size_t size = column->size();

    for (size_t i = 0; i < size; ++i)
    {
        /// TODO: support more types.
        Float64 value = column->getFloat64(i);
        data.add(value, 1);
    }
}

UInt64 TDigestStatistic::count()
{
    return static_cast<UInt64>(data.count);
}

}
