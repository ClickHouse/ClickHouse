#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <hll.hpp>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Core/Types.h>

namespace DB
{

const int DEFAULT_LG_K = 10;
const int MAX_LG_K = 21;
const auto DEFAULT_HLL_TYPE = datasketches::HLL_4; // this is the default, but explicit here for illustration

template <typename Key>
class HLLSketchData : private boost::noncopyable
{
private:
    std::unique_ptr<datasketches::hll_sketch> sk_update;
    std::unique_ptr<datasketches::hll_union> sk_union;

    uint8_t lg_k;
    datasketches::target_hll_type type;
    /// Whether lg_k / type were explicitly requested by the user (as opposed to being defaults).
    /// When they were not, the union must not degrade input sketches to the default precision:
    /// it is created with the maximum lg_k (an `hll_union` adapts downward to the smallest lg_k
    /// among its inputs, so this preserves each input sketch's precision), and the output
    /// representation is inferred from the first non-empty input sketch.
    bool lg_k_explicit;
    bool type_explicit;

    datasketches::hll_sketch * getHLLUpdate()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::hll_sketch>(datasketches::hll_sketch(lg_k, type));
        return sk_update.get();
    }

    datasketches::hll_union * getHLLUnion()
    {
        if (!sk_union)
            sk_union = std::make_unique<datasketches::hll_union>(datasketches::hll_union(lg_k_explicit ? lg_k : MAX_LG_K));
        return sk_union.get();
    }

    void adoptTypeFrom(const datasketches::hll_sketch & sk)
    {
        if (!type_explicit && !sk.is_empty())
        {
            type = sk.get_target_type();
            type_explicit = true;
        }
    }

public:
    using value_type = Key;

    HLLSketchData() : lg_k(DEFAULT_LG_K), type(DEFAULT_HLL_TYPE), lg_k_explicit(true), type_explicit(true) {}

    HLLSketchData(uint8_t lg_k_, datasketches::target_hll_type type_, bool lg_k_explicit_ = true, bool type_explicit_ = true)
        : lg_k(lg_k_), type(type_), lg_k_explicit(lg_k_explicit_), type_explicit(type_explicit_) {}

    ~HLLSketchData() = default;

    void insertOriginal(std::string_view value)
    {
        /// Match Apache DataSketches string semantics: `hll_sketch::update(const std::string &)`
        /// ignores empty strings, while the raw-bytes overload used below would hash a
        /// zero-length buffer and count it as a distinct element, producing sketches
        /// incompatible with native DataSketches string producers.
        if (value.empty())
            return;
        getHLLUpdate()->update(value.data(), value.size());
    }

    void insert(Key value)
    {
        /// DataSketches hll_sketch supports a limited set of primitive overloads.
        /// Types without a DataSketches-compatible encoding (e.g. Int256) are rejected
        /// by `serializedHLL` up front; the raw-bytes branch below remains only for
        /// template instantiations that are never reached from interoperable sketches.
        if constexpr (std::is_same_v<Key, BFloat16>)
        {
            getHLLUpdate()->update(static_cast<float>(value));
        }
        else if constexpr (std::is_floating_point_v<Key>)
        {
            getHLLUpdate()->update(static_cast<double>(value));
        }
        else if constexpr (std::is_integral_v<Key> && sizeof(Key) <= sizeof(UInt64))
        {
            /// Dispatch to the width-matching DataSketches overload. This is required for
            /// interoperability: DataSketches hashes uint8/16/32 through the same-width signed
            /// type (sign-extending it to int64), so widening them to UInt64 here would produce
            /// sketches incompatible with native DataSketches producers for values above the
            /// signed range of the type.
            if constexpr (std::is_signed_v<Key>)
                getHLLUpdate()->update(static_cast<Int64>(value));
            else if constexpr (sizeof(Key) == 1)
                getHLLUpdate()->update(static_cast<uint8_t>(value));
            else if constexpr (sizeof(Key) == 2)
                getHLLUpdate()->update(static_cast<uint16_t>(value));
            else if constexpr (sizeof(Key) == 4)
                getHLLUpdate()->update(static_cast<uint32_t>(value));
            else
                getHLLUpdate()->update(static_cast<uint64_t>(value));
        }
        else
        {
            getHLLUpdate()->update(&value, sizeof(value));
        }
    }

    UInt64 size() const
    {
        if (sk_union)
            return static_cast<UInt64>(sk_union->get_result().get_estimate());
        if (sk_update)
            return static_cast<UInt64>(sk_update->get_estimate());
        return 0;
    }

    void insertSerialized(std::string_view serialized_data, bool base64_encoded = false)
    {
        if (serialized_data.empty())
            return;

        std::string decoded_storage;
        /// When merging internally-generated sketches (from serializedHLL),
        /// we know the data is raw binary, not base64. Use base64_encoded=false for performance.
        /// For external data sources that might send base64, set base64_encoded=true.
        auto [data_ptr, data_size] = decodeSketchData(serialized_data, decoded_storage, base64_encoded);

        if (data_ptr == nullptr || data_size == 0)
            return;

        /// Deserialize and merge the sketch
        auto sk = deserializeSketch<datasketches::hll_sketch>(data_ptr, data_size);
        adoptTypeFrom(sk);
        getHLLUnion()->update(sk);
    }

    String serializedData()
    {
        if (sk_union)
        {
            /// Respect the configured target HLL type for the output sketch.
            /// This matters for users who explicitly request a specific representation (HLL_4/6/8)
            /// and keeps mergeSerializedHLL output consistent with serializedHLL settings.
            auto bytes = sk_union->get_result(type).serialize_compact();
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
        if (!type_explicit && rhs.type_explicit)
        {
            type = rhs.type;
            type_explicit = true;
        }

        /// Do not materialize a union when the other state holds no data. Otherwise a group
        /// consisting only of logically empty states would return a serialized empty sketch
        /// under partial (multi-stage) aggregation, while a single-stage aggregate returns
        /// an empty string - making the result plan-dependent.
        if (!rhs.sk_update && !rhs.sk_union)
            return;

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
            auto sk = deserializeSketch<datasketches::hll_sketch>(bytes.data(), bytes.size());
            adoptTypeFrom(sk);
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
            /// Serialize the partial state with the configured (possibly inferred) target type.
            /// `get_result` defaults to `HLL_4`, which would lose an inferred `HLL_6`/`HLL_8`
            /// representation across an aggregate-state round-trip (`read` re-adopts the type
            /// from the deserialized sketch), making the final sketch plan-dependent.
            auto bytes = sk_union->get_result(type).serialize_compact();
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
