#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <hll.hpp>

namespace DB
{

const int lg_k = 10;
const auto type = datasketches::HLL_4; // this is the default, but explicit here for illustration

template <typename Key>
class HLLSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::hll_sketch> sk_update;
    std::unique_ptr<datasketches::hll_union> sk_union;

    datasketches::hll_sketch * getHLLUpdate()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::hll_sketch>(datasketches::hll_sketch(lg_k, type));
        return sk_update.get();
    }

    datasketches::hll_union * getHLLUnion()
    {
        if (!sk_union)
            sk_union = std::make_unique<datasketches::hll_union>(datasketches::hll_union(lg_k));
        return sk_union.get();
    }

public:
    using value_type = Key;

    HLLSketchData() = default;
    ~HLLSketchData() = default;

    void insertOriginal(std::string_view value)
    {
        getHLLUpdate()->update(value.data(), value.size());
    }

    void insert(Key value)
    {
        getHLLUpdate()->update(value);
    }

    UInt64 size() const
    {
        if (sk_union)
            return static_cast<UInt64>(sk_union->get_result().get_estimate());
        if (sk_update)
            return static_cast<UInt64>(sk_update->get_estimate());
        return 0;
    }

	String serializedData()
    {
        if (sk_union)
		{
 			auto bytes = sk_union->get_result().serialize_compact();
            return String(bytes.begin(), bytes.end());
        }
        if (sk_update)
		{
            auto bytes = sk_update->serialize_compact();
            return String(bytes.begin(), bytes.end());
		}
        return "";
    }

    void merge(const HLLSketchData & rhs)
    {
        datasketches::hll_union * u = getHLLUnion();

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

    void read(DB::ReadBuffer & in)
    {
        datasketches::hll_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (!bytes.empty())
        {
            auto sk = datasketches::hll_sketch::deserialize(bytes.data(), bytes.size());
            getHLLUnion()->update(sk);
        }
    }

    void write(DB::WriteBuffer & out) const
    {
        if (sk_update)
        {
            auto bytes = sk_update->serialize_compact();
            writeVectorBinary(bytes, out);
        }
        else if (sk_union)
        {
            auto bytes = sk_union->get_result().serialize_compact();
            writeVectorBinary(bytes, out);
        }
        else
        {
            datasketches::hll_sketch::vector_bytes bytes;
            writeVectorBinary(bytes, out);
        }
    }
};


}

#endif
