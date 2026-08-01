#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationInterval.h>


namespace DB
{

SerializationInterval::SerializationInterval(IntervalKind interval_kind_) : interval_kind(std::move(interval_kind_))
{
}


UInt128 SerializationInterval::getHash(IntervalKind kind_)
{
    SipHash hash;
    hash.update("Interval");
    hash.update(kind_.toString());
    return hash.get128();
}

SerializationPtr SerializationInterval::create(IntervalKind kind_)
{
    return ISerialization::pooled(getHash(kind_), [=] { return new SerializationInterval(kind_); });
}

}
