#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/LongConnection.h>
#include <IO/ReaderExecutorFetchMachine.h>

#include <optional>

namespace DB
{

/// The FILL LANE - the WRITE side of the executor's cache-as-pipe (the display is the read
/// side). Owns everything on the fill side: THE long source connection, the in-flight
/// segment pin, the ahead launch cursor, and the bank. All offsets are PHYSICAL.
class ReaderExecutorFillLane
{
public:
    /// The lane holds THE long connection. A POOL piece borrows it for its flight
    /// (`lend`/`reclaim` are the only movers); an INLINE piece runs on the serve thread and
    /// uses the slot directly - no borrow. While the connection is LENT the lane refuses to
    /// open another (`shouldOpenLongConnection`), so a foreground read during a pool flight
    /// degrades to a one-shot: "the foreground holds no connection mid-flight" is lane
    /// state, not a chassert at every collect site.
    std::optional<LongConnection> conn;
    bool conn_lent = false;

    void lend(ReaderExecutorFetchMachine & m)
    {
        chassert(!m.long_conn);
        m.long_conn = takeLongConnection(conn);
        conn_lent = m.long_conn.has_value();
    }

    /// The worker no longer touches the payload (queued-cancel, or the release edge has
    /// passed). The lane cannot hold a second connection meanwhile - opens are refused
    /// while lent - which is what makes this a move, never an overwrite.
    void reclaim(ReaderExecutorFetchMachine & m)
    {
        chassert(!(conn && m.long_conn));
        if (m.long_conn)
            conn = takeLongConnection(m.long_conn);
        conn_lent = false;
    }

    /// The AHEAD cursor `F` - ONE global launch high-water: everything below it has been
    /// attempted by this executor (launched over, served inline, or observed covered)
    /// whether the bytes committed, were refused, or belong to a sibling's download.
    /// Launch POLICY only - the serve reads the display. Plan-scoped: REUSE and
    /// EXTEND keep it with the surviving plan; a RESTART re-derives it.
    size_t attempted_end = 0;
    void advanceAttempted(size_t phys_end) { attempted_end = std::max(attempted_end, phys_end); }

    /// The BANK - the pipe's overflow cell: bytes no cache cell could hold (a bypass gap's
    /// fetch, refused writes, sibling-waited chunks, heal reads), consumed-and-trimmed as
    /// the display serves. ONE lane-level holder: the display reads it by offset, so job
    /// identity carries nothing. Plan-scoped: dropped on RESTART with the ahead
    /// cursor, trimmed to `bank_keep_behind` behind the serve cursor as it serves.
    ChainedBuffers bank;

    /// How far behind the serve cursor banked bytes are RETAINED instead of trimmed:
    /// the REUSE reach (`min_bytes_for_seek`, set at executor construction) - a near
    /// seek may swing back into it. 0 = trim to the served prefix (the pre-reuse rule).
    size_t bank_keep_behind = 0;

    /// Banked bytes AT/AFTER `cursor` - the unconsumed-ahead holding the launch
    /// backpressure budgets against. Behind-retention bytes are deliberately
    /// excluded, so `bank_keep_behind` cannot latch the budget shut.
    size_t bankAheadBytes(size_t cursor) const
    {
        size_t total = 0;
        for (const auto & iv : bank.getIntervals())
            if (iv.end() > cursor)
                total += iv.end() - std::max(iv.offset, cursor);
        return total;
    }

    /// RESTART: the ahead cursor re-derives from the fresh display truth and
    /// the bank drops with the plan it served.
    void resetOnRestart()
    {
        attempted_end = 0;
        bank = {};
    }

    /// Fold the bank's coverage clamped to `window` into `cov` - per INTERVAL, never the
    /// bounding range: the bank can hold disjoint chunks (sibling-waited pieces), and
    /// coverage must never claim a hole.
    void addBankCoverage(IntervalSet & cov, ByteRange window) const
    {
        for (const auto & iv : bank.getIntervals())
        {
            const size_t lo = std::max(iv.offset, window.offset);
            const size_t hi = std::min(iv.end(), window.end());
            if (lo < hi)
                cov.add(ByteRange{lo, hi - lo});
        }
    }
};

}
