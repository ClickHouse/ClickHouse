#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <hll.hpp>
#include <AggregateFunctions/SketchDataUtils.h>

namespace DB
{

const int DEFAULT_LG_K = 10;
const auto DEFAULT_HLL_TYPE = datasketches::HLL_4; // this is the default, but explicit here for illustration

template <typename Key>
class HLLSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::hll_sketch> sk_update;
    std::unique_ptr<datasketches::hll_union> sk_union;

    uint8_t lg_k;
    datasketches::target_hll_type type;

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

    HLLSketchData() : lg_k(DEFAULT_LG_K), type(DEFAULT_HLL_TYPE) {}

    HLLSketchData(uint8_t lg_k_, datasketches::target_hll_type type_)
        : lg_k(lg_k_), type(type_) {}

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

    void insertSerialized(std::string_view serialized_data, bool force_raw = true)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_storage;
        /// When merging internally-generated sketches (from serializedHLL),
        /// we know the data is raw binary, not base64. Use force_raw=true for performance.
        /// For external data sources that might send base64, set force_raw=false.
        auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, force_raw);

        if (data_ptr == nullptr || data_size == 0)
            return;

        /// Deserialize and merge the sketch
        try
        {
            auto sk = datasketches::hll_sketch::deserialize(data_ptr, data_size);
            getHLLUnion()->update(sk);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// If deserialization fails (corrupted or invalid data), skip this value.
            /// This allows graceful handling of bad input data rather than failing the entire aggregation.
        }
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
