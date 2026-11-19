#pragma once

#include "config.h"

#if USE_DATASKETCHES

#include <boost/noncopyable.hpp>
#include <memory>
#include <theta_sketch.hpp>
#include <theta_union.hpp>
#include <theta_intersection.hpp>
#include <theta_a_not_b.hpp>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


template <typename Key>
class ThetaSketchData : private boost::noncopyable
{
private:
    /// Used for insertions
    std::unique_ptr<datasketches::update_theta_sketch> sk_update;
    /// Used for merging
    std::unique_ptr<datasketches::theta_union> sk_union;

    datasketches::update_theta_sketch * getSkUpdate()
    {
        if (!sk_update)
            sk_update = std::make_unique<datasketches::update_theta_sketch>(datasketches::update_theta_sketch::builder().build());
        return sk_update.get();
    }

    datasketches::theta_union * getSkUnion()
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
    void insertOriginal(std::string_view value)
    {
        getSkUpdate()->update(value.data(), value.size());
        /// In case of optimization for u8 keys (see addBatchLookupTable()) it is possible to have few calls of insert() after merge(),
        /// and we should update sk_union as well, note, that there should not be too many, so performance wise it should be OK
        if (sk_union)
        {
            sk_union->update(*sk_update);
            sk_update.reset(nullptr);
        }
    }

    /// Note that `datasketches::update_theta_sketch.update` will do the hash again.
    void insert(Key value)
    {
        getSkUpdate()->update(value);
        /// In case of optimization for u8 keys (see addBatchLookupTable()) it is possible to have few calls of insert() after merge(),
        /// and we should update sk_union as well, note, that there should not be too many, so performance wise it should be OK
        if (sk_union)
        {
            sk_union->update(*sk_update);
            sk_update.reset(nullptr);
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
        /// If `rhs` has no recorded values it represents an empty set, and the
        /// intersection with an empty set is always empty. Without this guard
        /// `theta_intersection` would only receive `this` as a single input and
        /// `get_result` would return that input unchanged — yielding a wrong,
        /// non-empty result. This matters for example after
        /// `uniqThetaMergeStateIf(state, predicate)` when the predicate excludes
        /// every row: the resulting state is freshly created and has neither
        /// `sk_update` nor `sk_union` allocated.
        if (!rhs.sk_update && !rhs.sk_union)
        {
            sk_update.reset(nullptr);
            sk_union.reset(nullptr);
            return;
        }

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

    void aNotB(const ThetaSketchData & rhs)
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

    /// The number of leading bytes `compact_theta_sketch_parser::parse` reads before it checks that the
    /// buffer holds them, derived from the first 8 bytes - the only ones it does check for up front. The
    /// offsets are the ones of `compact_theta_sketch_parser`; every serial version and preamble size not
    /// listed here is either checked by the parser itself before each read or rejected by it from the
    /// first 8 bytes.
    static size_t headerBytesReadBeforeSizeCheck(const uint8_t * data, size_t size)
    {
        static constexpr size_t header_size = 8;
        if (size < header_size)
            return header_size;

        const uint8_t preamble_longs = data[0];
        const uint8_t serial_version = data[1];
        const uint8_t flags = data[5];
        static constexpr uint8_t is_empty_flag = 1 << 2;

        switch (serial_version)
        {
            /// `num_entries` at offset 8 and `theta` at offset 16, unconditionally.
            case 1:
                return 24;
            /// `num_entries` at offset 8 for two preamble longs, `theta` at offset 16 as well for three.
            case 2:
                if (preamble_longs == 2)
                    return 16;
                if (preamble_longs == 3)
                    return 24;
                return header_size;
            /// `num_entries` at offset 8 unless the sketch is flagged empty; a single-entry sketch
            /// (one preamble long) and `theta` (more than two) are checked by the parser itself.
            case 3:
                if ((flags & is_empty_flag) || preamble_longs == 1 || preamble_longs > 2)
                    return header_size;
                return 16;
            default:
                return header_size;
        }
    }

    /// You can only call for an empty object.
    void read(DB::ReadBuffer & in)
    {
        datasketches::compact_theta_sketch::vector_bytes bytes;
        readVectorBinary(bytes, in);
        if (bytes.empty())
            return;

        /** `compact_theta_sketch_parser::parse` verifies that the buffer holds 8 bytes and then reads
          * header fields that lie beyond them before it checks the size again: `num_entries` at offset
          * 8 for serial versions 1, 2 and 3, and `theta` at offset 16 for serial versions 1 and 2. A
          * state shorter than that - which any `CAST` from a string can produce - is therefore read
          * past its end, and the out-of-bounds value decides the size the parser then demands.
          *
          * Require up front the bytes the parser reads before it validates anything, exactly as the
          * `std::istream` deserializer of `datasketches` has to consume them before it can decide
          * anything about the sketch. A state that stops short of them is refused the same way the
          * parser refuses one that stops short of its entries, and the parser then never reads past
          * the buffer. The upstream fix is to move each `check_memory_size` before the field it
          * guards; this keeps the read in bounds for every version of `datasketches-cpp` this
          * repository pulls in.
          */
        const size_t required_size = headerBytesReadBeforeSizeCheck(bytes.data(), bytes.size());
        if (bytes.size() < required_size)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Cannot deserialize Theta sketch state: at least {} bytes expected, actual {}",
                required_size,
                bytes.size());

        try
        {
            auto sk = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
            getSkUnion()->update(sk);
        }
        catch (const DB::Exception &)
        {
            throw;
        }
        catch (const std::bad_alloc &)
        {
            /// Memory pressure on `compact_theta_sketch::deserialize`, `getSkUnion`,
            /// or `theta_union.update` is not data corruption; let it propagate.
            throw;
        }
        catch (const std::exception & e)
        {
            /// `datasketches` throws `std::invalid_argument` / `std::out_of_range` on
            /// malformed input. These are not `DB::Exception`, so without translation
            /// they escape `SerializationAggregateFunction`'s `catch (...)` block, reach
            /// the top level as `LOGICAL_ERROR` (code 1001), and abort the process via
            /// `abortOnFailedAssertion`. Translate to `CORRUPTED_DATA` so the bad input
            /// is rejected cleanly.
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Cannot deserialize Theta sketch state: {}",
                e.what());
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
