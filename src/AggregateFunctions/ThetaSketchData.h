#pragma once

#include <boost/noncopyable.hpp>
#include <theta_sketch.hpp>
#include <theta_union.hpp>

#include <memory>

namespace DB
{


template <typename Key>
class ThetaSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::update_theta_sketch> sk_update;
    std::unique_ptr<datasketches::theta_union> sk_union;

    inline datasketches::update_theta_sketch * get_sk_update()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::update_theta_sketch>(datasketches::update_theta_sketch::builder().build());
        return sk_update.get();
    }
    
    inline datasketches::theta_union * get_sk_union()
    {
        if (!sk_union)
            sk_union = std::make_unique<datasketches::theta_union>(datasketches::theta_union::builder().build());
        return sk_union.get();
    }

public:
    using value_type = Key;

    ThetaSketchData() = default;
    ~ThetaSketchData() = default;

    /// Insert original value without hash, as `datasketches::update_theta_sketch.update` will do the hash internal.
    void insert_original(const StringRef & value)
    {
        get_sk_update()->update(value.data, value.size);
    }

    /// Note that `datasketches::update_theta_sketch.update` will do the hash again.
    void insert(Key value)
    {
        get_sk_update()->update(value);
    }

    UInt64 size() const
    {
        if (sk_union)
            return static_cast<UInt64>(sk_union->get_result().get_estimate());
        else if (sk_update)
            return static_cast<UInt64>(sk_update->get_estimate());
        else
            return 0;
    }

    void merge(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = get_sk_union();
        
        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        if (rhs.sk_update)
            u->update(*rhs.sk_update);
        else if (rhs.sk_union)
            u->update(rhs.sk_union->get_result());
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        datasketches::compact_theta_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
            get_sk_union()->update(sk);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (sk_update)
        {
            auto bytes = sk_update->compact().serialize();
            writeVectorBinary(bytes, out);
        }
        else if (sk_union)
        {
            auto bytes = sk_union->get_result().serialize();
            writeVectorBinary(bytes, out);
        }
        else
        {
            datasketches::compact_theta_sketch::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }
};


}
