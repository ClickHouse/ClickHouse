#pragma once

#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int INCORRECT_DATA;
}

/// Bridges ReadBuffer page boundaries for TurboPFor block-at-a-time decoding.
///
/// TurboPFor decoders (p4D1Dec256v32, p4D1Dec32, etc.) require contiguous input
/// memory, but ReadBuffer may split encoded data across internal pages. This class
/// provides a contiguous view via ptr()/advance() with a zero-copy fast path and
/// efficient carry-over for page-boundary crossings.
///
/// Usage:
///
///   TurboPForBlockDecodeBuffer dbuf(in);
///   while (has_more_blocks)
///   {
///       const uint8_t * p = dbuf.ptr();
///       const uint8_t * end = turbopfor::p4D1Dec256v32(p, 256, out, prev);
///       dbuf.advance(end - p);
///   }
///
/// ═══════════════════════════════════════════════════════════════════
///  ptr() flow
/// ═══════════════════════════════════════════════════════════════════
///
///   ptr() called
///   │
///   ├─ 1. Have carry buffer and buf_pos < carry_end?
///   │     → return buf + buf_pos (zero copy, zero I/O)
///   │
///   ├─ 2. Carry exhausted (carry_end > 0 and buf_pos >= carry_end)?
///   │     │
///   │     │  leftover = buffered - buf_pos
///   │     │  These bytes sit at buf tail, copied earlier from next page(s).
///   │     │  in.position() has already been advanced past them.
///   │     │
///   │     ├─ 2a. Can return leftover to ReadBuffer?
///   │     │      (in.position() - in.buffer().begin() >= leftover)
///   │     │      in.position() -= leftover
///   │     │      Clear carry state
///   │     │      Fall through to step 3
///   │     │
///   │     └─ 2b. Cannot return (crossed multiple small pages)?
///   │            memmove(buf, buf + buf_pos, leftover)
///   │            Fill MAX bytes from ReadBuffer after leftover
///   │            buf_pos = 0, carry_end = leftover + 1
///   │            buffered = leftover + filled
///   │            return buf.data()
///   │
///   ├─ 3. in.available() >= MAX? (fast path)
///   │     in_place = true
///   │     return in.position() (zero copy)
///   │
///   └─ 4. in.available() < MAX (new carry)
///         tail = in.available()
///         copy buf[0..tail) ← in.position()
///         in.position() += tail (exhaust current page)
///         Fill MAX bytes from ReadBuffer into buf[tail..)
///         buf_pos = 0
///         carry_end = tail + 1
///         buffered = tail + filled
///         return buf.data()
///
/// ═══════════════════════════════════════════════════════════════════
///  carry_end invariant
/// ═══════════════════════════════════════════════════════════════════
///
///   buf after fill:
///     [TTTTT|HHHHHHHHHHHHHHHHHH]
///      tail   MAX bytes from next page(s)
///      ^     ^                  ^
///      0   carry_end          buffered
///
///   carry_end = tail + 1
///
///   Position 0:    tail + MAX bytes ahead  ≥ MAX  ✓
///   Position tail: MAX bytes ahead         ≥ MAX  ✓
///   Position tail+1: MAX - 1 bytes ahead   < MAX  ✗ ← carry_end
///
///   So buf_pos < carry_end ≡ "at least MAX bytes from buf_pos to end".
///
/// ═══════════════════════════════════════════════════════════════════
///  advance(n) flow
/// ═══════════════════════════════════════════════════════════════════
///
///   ├─ in_place? → in.position() += n, in_place = false
///   └─ carry?   → buf_pos += n
///
/// ═══════════════════════════════════════════════════════════════════
///  Performance
/// ═══════════════════════════════════════════════════════════════════
///
///   • Fast path (common): zero copy, ptr() returns ReadBuffer pointer.
///   • Carry entry (rare, once per page boundary): memcpy of tail + MAX
///     bytes. Carry serves multiple decodes before exhaustion.
///   • Carry exhaustion with return (common for large pages): position
///     rollback, then back to fast path. Zero memmove.
///   • Carry exhaustion with compact (rare, tiny pages only): memmove
///     of leftover (< MAX ≈ 1 KB), then refill from ReadBuffer.
///   • advance() is always O(1): pointer increment, never copies.
///
/// ═══════════════════════════════════════════════════════════════════
///  EOF handling
/// ═══════════════════════════════════════════════════════════════════
///
///   At end-of-stream, the fill loop may yield fewer than MAX bytes.
///   carry_end is set to 1 (only position 0 is valid). The tail-block
///   decoder (p4D1Dec32) reads exactly the encoded bytes without
///   overreading past them.
class TurboPForBlockDecodeBuffer
{
public:
    explicit TurboPForBlockDecodeBuffer(ReadBuffer & in_)
        : in(in_)
    {
    }

    /// Returns a pointer to contiguous memory where one TurboPFor block can
    /// be safely decoded. Valid until the next ptr() call.
    const uint8_t * ptr()
    {
        /// Step 1: Still in carry region — guaranteed sufficient lookahead.
        if (buf_pos < carry_end)
            return buf.data() + buf_pos;

        /// Step 2: Carry exhausted — try to return unconsumed bytes to ReadBuffer.
        if (carry_end > 0)
        {
            size_t leftover = buffered - buf_pos;
            size_t rewind_room = static_cast<size_t>(in.position() - in.buffer().begin());

            if (leftover <= rewind_room)
            {
                /// 2a: All leftover bytes came from the current ReadBuffer page.
                ///     Roll back position so ReadBuffer owns them again.
                in.position() -= leftover;
                buf_pos = 0;
                carry_end = 0;
                buffered = 0;
                /// Fall through to step 3/4.
            }
            else
            {
                /// 2b: Leftover spans multiple pages — cannot roll back.
                ///     Compact to front and refill.
                if (leftover > 0)
                    memmove(buf.data(), buf.data() + buf_pos, leftover);

                size_t target = leftover + TURBOPFOR_MAX_ENCODED_SIZE_64;
                if (buf.size() < target)
                    buf.resize(target);

                size_t have = leftover;
                while (have < target && !in.eof())
                {
                    size_t can = std::min(in.available(), target - have);
                    memcpy(buf.data() + have, in.position(), can);
                    in.position() += can;
                    have += can;
                }

                /// See `ptr()` step 4 for the rationale: zero-pad the unfilled tail so
                /// decoder over-reads stay deterministic and `advance()` correctly rejects
                /// over-consumption.
                if (have < target)
                    std::memset(buf.data() + have, 0, target - have);

                buf_pos = 0;
                buffered = have;
                in_place = false;

                if (have >= leftover + TURBOPFOR_MAX_ENCODED_SIZE_64)
                    carry_end = leftover + 1;
                else
                    carry_end = 1;

                return buf.data();
            }
        }

        /// Step 3: Fast path — ReadBuffer page has enough contiguous data.
        if (!in.eof() && in.available() >= TURBOPFOR_MAX_ENCODED_SIZE_64)
        {
            in_place = true;
            return reinterpret_cast<const uint8_t *>(in.position());
        }

        /// Step 4: New carry — copy page tail, then fill MAX from next page(s).
        size_t tail = in.available();
        size_t target = tail + TURBOPFOR_MAX_ENCODED_SIZE_64;
        if (buf.size() < target)
            buf.resize(target);

        memcpy(buf.data(), in.position(), tail);
        in.position() += tail;

        size_t have = tail;
        while (have < target && !in.eof())
        {
            size_t can = std::min(in.available(), target - have);
            memcpy(buf.data() + have, in.position(), can);
            in.position() += can;
            have += can;
        }

        /// Zero-pad the unfilled tail so any decoder over-read into the padding stays
        /// deterministic. Without this `PODArray::resize` leaves the trailing bytes
        /// uninitialised, and TurboPFor's loadU64Fast (which over-reads up to 7 bytes)
        /// would see stale data and decode garbage that `advance()` could still mark
        /// as legitimately consumed. The cost is bounded by the padding region only,
        /// which is at most `TURBOPFOR_MAX_ENCODED_SIZE_64` (~2 KB) and only runs on the
        /// rare EOF carry path — full-page fast paths skip this code entirely.
        if (have < target)
            std::memset(buf.data() + have, 0, target - have);

        buf_pos = 0;
        buffered = have;
        in_place = false;

        if (have >= tail + TURBOPFOR_MAX_ENCODED_SIZE_64)
            carry_end = tail + 1;
        else
            carry_end = 1;

        return buf.data();
    }

    /// Mark `consumed` bytes as used after decoding one block.
    /// Throws `INCORRECT_DATA` if the decoder over-advanced past what was actually buffered
    /// (e.g. a full-block decode invoked on a truncated EOF buffer would read uninitialised
    /// trailing bytes and then claim more bytes than were ever available).
    void advance(size_t consumed)
    {
        if (in_place)
        {
            if (consumed > in.available()) [[unlikely]]
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Corrupted projection text index: TurboPFor decoder consumed {} bytes from in-place buffer with only {} available",
                    consumed, in.available());
            in.position() += consumed;
            in_place = false;
            return;
        }
        if (buf_pos + consumed > buffered) [[unlikely]]
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Corrupted projection text index: TurboPFor decoder consumed {} bytes from carry buffer at position {} with only {} buffered",
                consumed, buf_pos, buffered);
        buf_pos += consumed;
    }

    /// Invalidate carry state. Must be called after seek() on the underlying ReadBuffer.
    void reset()
    {
        buf_pos = 0;
        carry_end = 0;
        buffered = 0;
        in_place = false;
    }

    /// Roll back the prefetched-but-not-decoded bytes to the underlying ReadBuffer,
    /// so the caller can resume reading from `in` directly at the logical decode
    /// position. Must be called before mixing dbuf decodes with raw reads from `in`.
    /// Throws if the prefetch spans multiple pages and cannot be rewound.
    void sync()
    {
        if (in_place)
        {
            in_place = false;
            return;
        }
        size_t leftover = buffered - buf_pos;
        if (leftover == 0)
        {
            buf_pos = 0;
            carry_end = 0;
            buffered = 0;
            return;
        }
        size_t rewind_room = static_cast<size_t>(in.position() - in.buffer().begin());
        if (leftover > rewind_room)
            throw Exception(
                ErrorCodes::CANNOT_READ_ALL_DATA,
                "TurboPForBlockDecodeBuffer::sync: prefetch spans multiple pages, cannot rewind {} bytes (room: {})",
                leftover,
                rewind_room);
        in.position() -= leftover;
        buf_pos = 0;
        carry_end = 0;
        buffered = 0;
    }

private:
    ReadBuffer & in;
    PODArray<uint8_t> buf; /// carry-over buffer
    size_t buf_pos = 0; /// current read position within buf
    size_t carry_end = 0; /// exclusive: buf_pos < carry_end → safe to decode
    size_t buffered = 0; /// total valid bytes in buf
    bool in_place = false; /// last ptr() returned in.position() directly
};

}
