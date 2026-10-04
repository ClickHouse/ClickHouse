#pragma once

/// Countdown tickers (jemalloc: `ticker.h`, `ticker.c`).

#include <allocator/Common.h>
#include <allocator/Prng.h>

#include <cstdint>

namespace jemalloc
{

/// Counts down events until some limit: initialized to trigger every `nticks` events, `tick`/`ticks` return true
/// (and reset the counter) when the countdown goes below zero.
/// jemalloc: ticker_t
struct Ticker
{
    int32_t tick;
    int32_t nticks;

    /// jemalloc: ticker_init
    JE_ALWAYS_INLINE void init(int32_t nticks_)
    {
        tick = nticks_;
        nticks = nticks_;
    }

    /// jemalloc: ticker_copy
    JE_ALWAYS_INLINE void copyFrom(const Ticker & other) { *this = other; }

    /// jemalloc: ticker_read
    JE_ALWAYS_INLINE int32_t read() const { return tick; }

    /// Not intended to be called directly.
    /// jemalloc: ticker_fixup
    JE_ALWAYS_INLINE bool fixup(bool delay_trigger)
    {
        if (delay_trigger)
        {
            tick = 0;
            return false;
        }
        tick = nticks;
        return true;
    }

    /// jemalloc: ticker_ticks
    JE_ALWAYS_INLINE bool ticks(int32_t n, bool delay_trigger)
    {
        tick -= n;
        if (JE_UNLIKELY(tick < 0))
            return fixup(delay_trigger);
        return false;
    }

    /// jemalloc: ticker_tick
    JE_ALWAYS_INLINE bool tickOnce(bool delay_trigger) { return ticks(1, delay_trigger); }

    /// Try to tick. If the ticker would fire, return true, but rely on the slow path to reset it.
    /// jemalloc: ticker_trytick
    JE_ALWAYS_INLINE bool tryTick()
    {
        --tick;
        if (JE_UNLIKELY(tick < 0))
            return true;
        return false;
    }
};

static_assert(sizeof(Ticker) == 8);

/// The geometric distribution table (computed by jemalloc's `src/ticker.py`): to avoid floating point on the core
/// paths, ceil(log(u) / log(1 - 1/nticks)) for u uniform in [1/64, 1] is approximated by
/// nticks * table[u] / TICKER_GEOM_MUL.
inline constexpr unsigned TICKER_GEOM_NBITS = 6;
inline constexpr uint64_t TICKER_GEOM_MUL = 61;

/// jemalloc: ticker_geom_table
inline constexpr uint8_t ticker_geom_table[1 << TICKER_GEOM_NBITS] = {254, 211, 187, 169, 156, 144, 135, 127, 120, 113,
    107, 102, 97, 93, 89, 85, 81, 77, 74, 71, 68, 65, 62, 60, 57, 55, 53, 50, 48, 46, 44, 42, 40, 39, 37, 35, 33, 32, 30,
    29, 27, 26, 24, 23, 21, 20, 19, 18, 16, 15, 14, 13, 12, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

/// Like `Ticker`, but each tick has approximately a 1/nticks chance of firing (the countdown after firing is drawn
/// from the geometric distribution using the caller's PRNG state). Used to trigger arena decay with a single ticker
/// per thread instead of one per (thread, arena).
/// jemalloc: ticker_geom_t
struct TickerGeom
{
    int32_t tick;
    int32_t nticks;

    /// jemalloc: ticker_geom_init
    JE_ALWAYS_INLINE void init(int32_t nticks_)
    {
        /// Make sure there's no overflow possible.
        JE_ASSERT(uint64_t(nticks_) * uint64_t(255) / TICKER_GEOM_MUL <= uint64_t(INT32_MAX));
        tick = nticks_;
        nticks = nticks_;
    }

    /// jemalloc: ticker_geom_read
    JE_ALWAYS_INLINE int32_t read() const { return tick; }

    /// jemalloc: ticker_geom_fixup
    JE_ALWAYS_INLINE bool fixup(uint64_t & prng_state, bool delay_trigger)
    {
        if (delay_trigger)
        {
            tick = 0;
            return false;
        }
        uint64_t idx = prngLgRangeU64(prng_state, TICKER_GEOM_NBITS);
        tick = static_cast<int32_t>(static_cast<uint32_t>(uint64_t(nticks) * uint64_t(ticker_geom_table[idx]) / TICKER_GEOM_MUL));
        return true;
    }

    /// jemalloc: ticker_geom_ticks
    JE_ALWAYS_INLINE bool ticks(uint64_t & prng_state, int32_t n, bool delay_trigger)
    {
        tick -= n;
        if (JE_UNLIKELY(tick < 0))
            return fixup(prng_state, delay_trigger);
        return false;
    }

    /// jemalloc: ticker_geom_tick
    JE_ALWAYS_INLINE bool tickOnce(uint64_t & prng_state, bool delay_trigger) { return ticks(prng_state, 1, delay_trigger); }
};

static_assert(sizeof(TickerGeom) == 8);

/// Just pick the average delay for the first counter.
/// jemalloc: TICKER_GEOM_INIT
consteval TickerGeom tickerGeomInit(int32_t nticks)
{
    return TickerGeom{nticks, nticks};
}

}
