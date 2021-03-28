#pragma once

#include <boost/noncopyable.hpp>
#include <theta_sketch.hpp>
#include <theta_union.hpp>

#include <common/logger_useful.h>

namespace DB
{


template
<
    typename Key,
    typename Hash = IntHash32<Key>>
class ThetaSketchData : private boost::noncopyable
{
private:
    mutable datasketches::update_theta_sketch sk_update;
    mutable datasketches::theta_union sk_union;
    Poco::Logger * log;

    void internal_merge() const
    {
        if (!sk_update.is_empty())
        {
            sk_union.update(sk_update);
            sk_update = datasketches::update_theta_sketch::builder().build();
        }
    }

public:
    using value_type = Key;

    ThetaSketchData()
        : sk_update(datasketches::update_theta_sketch::builder().build()),
          sk_union(datasketches::theta_union::builder().build()),
          log(&Poco::Logger::get("ThetaSketchData"))
    {
    }
    ~ThetaSketchData() = default;

    /// Insert original value without hash, as `datasketches::update_theta_sketch.update` will do the hash internal.
    void insert_original(StringRef value)
    {
        sk_update.update(value.data, value.size);
        LOG_WARNING(log, "insert_origin() {}, {}", value.toString(), sk_update.to_string());
    }

    /// Note that `datasketches::update_theta_sketch.update` will do the hash again.
    void insert(Key value)
    {
        sk_update.update(value);
        LOG_WARNING(log, "insert() {}, {}", value, sk_update.to_string());
    }

    UInt64 size() const
    {
        LOG_WARNING(log, "size() update:{}, union:{}", sk_update.get_estimate(), sk_union.get_result().get_estimate());
        internal_merge();
        return static_cast<UInt64>(sk_union.get_result().get_estimate());
    }

    void merge(const ThetaSketchData & rhs)
    {
        rhs.internal_merge();
        sk_union.update(rhs.sk_union.get_result());

        LOG_WARNING(log, "merge() result:{}, rhs:{}", sk_union.get_result().to_string(), rhs.sk_union.get_result().to_string());
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        datasketches::compact_theta_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());

        sk_union = datasketches::theta_union::builder().build();
        sk_union.update(sk);

        LOG_WARNING(log, "read() {}", sk_union.get_result().to_string());
    }

    void readAndMerge(DB::ReadBuffer &)
    {
        assert(0);
    }

    void write(DB::WriteBuffer & out) const
    {
        internal_merge();
        auto bytes = sk_union.get_result().serialize();
        writeVectorBinary(bytes, out);

        LOG_WARNING(log, "write() {}", sk_union.get_result().to_string());
    }
};


}
