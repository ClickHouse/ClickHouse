#pragma once

#include <Common/config.h>

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <base/StringRef.h>
#include <theta_sketch.hpp>
#include <theta_union.hpp>
#include <theta_intersection.hpp>
#include <theta_a_not_b.hpp>


namespace DB
{


template <typename Key>
class ThetaSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::update_theta_sketch> sk_update;
    std::unique_ptr<datasketches::theta_union> sk_union;

    inline datasketches::update_theta_sketch * getSkUpdate()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::update_theta_sketch>(datasketches::update_theta_sketch::builder().build());
        return sk_update.get();
    }

    inline datasketches::theta_union * getSkUnion()
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
    void insertOriginal(StringRef value)
    {
        getSkUpdate()->update(value.data, value.size);
    }

    /// Note that `datasketches::update_theta_sketch.update` will do the hash again.
    void insert(Key value)
    {
        getSkUpdate()->update(value);
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
        datasketches::theta_union * u = getSkUnion();

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

    void intersect(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = getSkUnion();

        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        datasketches::theta_intersection theta_intersection;

        theta_intersection.update(u->get_result());

        if (rhs.sk_update)
            theta_intersection.update(*rhs.sk_update);
        else if (rhs.sk_union)
            theta_intersection.update(rhs.sk_union->get_result());

        sk_union.reset(nullptr);
        u = getSkUnion();
        u->update(theta_intersection.get_result());
    }

    void a_not_b(const ThetaSketchData & rhs)
    {
        datasketches::theta_union * u = getSkUnion();

        if (sk_update)
        {
            u->update(*sk_update);
            sk_update.reset(nullptr);
        }

        datasketches::theta_a_not_b a_not_b;

        if (rhs.sk_update)
        {
            datasketches::compact_theta_sketch result = a_not_b.compute(u->get_result(), *rhs.sk_update);
            sk_union.reset(nullptr);
            u = getSkUnion();
            u->update(result);
        }
        else if (rhs.sk_union)
        {
            datasketches::compact_theta_sketch result = a_not_b.compute(u->get_result(), rhs.sk_union->get_result());
            sk_union.reset(nullptr);
            u = getSkUnion();
            u->update(result);
        }
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        datasketches::compact_theta_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
            getSkUnion()->update(sk);
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

#endif
