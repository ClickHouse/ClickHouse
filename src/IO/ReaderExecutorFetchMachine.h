#pragma once

#include <IO/ChainedBuffers.h>
#include <condition_variable>
#include <mutex>
#include <IO/FetchMachine.h>
#include <IO/ICacheProvider.h>
#include <IO/LongConnection.h>
#include <IO/ReaderExecutorStats.h>
#include <Common/CurrentMetrics.h>
#include <Common/MemoryPressureMonitor.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

/// A NON-OWNING reference to one held write buffer in `read_plan.tiers`
/// (`writer` is owned there by a `unique_ptr`). A put step records these
/// instead of moving the writers out, so the shared `tiers` are written in
/// place. The raw pointer stays valid while `read_plan` is not rebuilt - and
/// every rebuild path drains the put lane first - so a put never outlives its
/// views. `range` is the cell's miss range (the lane-overlap key).
struct ReaderExecutorWriterView
{
    CacheWriter * writer = nullptr;
    ByteRange range;
};

/// The model's in-flight PIECE token: a steppable context for one fill piece,
/// pool-run (ahead) or serve-thread-run (pump). One step today - elect the
/// downloaders, fetch the LED runs of the pre-bounded window, stream them into
/// the cells - then the `AwaitCollect` barrier; the foreground collects or
/// cancels. Bundles EVERYTHING the worker touches, so it never reads a shared
/// `this->` member.
struct ReaderExecutorFetchMachine : MachineBase
{
    /// Out-of-line: initializes `inflight_gauge` (metric symbol is in the .cpp).
    ReaderExecutorFetchMachine();

    /// The PHYSICAL cache-aligned window the fetch step reads (cut at cell
    /// edges at launch), committing cells per tile as it goes. The LOGICAL
    /// requested range (the space `position` works in) is this shifted down
    /// by `data_start_offset`.
    ByteRange physical_window;
    /// The plan's memory-pressure level, snapshotted at launch - the only
    /// geometry field the worker reads (sizes the fetch block / suppresses
    /// read-ahead). A future stage needing other geometry fields on the
    /// worker must re-add a snapshot rather than reach into shared state.
    MemoryPressureLevel pressure_snapshot{};
    /// Whether a read extent was advertised at launch: the worker reads THIS,
    /// never the live `read_extent_end` member - a soft-cancelled machine must
    /// not race `setReadExtent`. Only the bit matters: it lets a one-shot read
    /// on an unknown-size source be bounded (`readFromSource`); the serve is
    /// bounded by the live extent separately (`clampToExtent`).
    bool extent_advertised = false;
    /// The schedule retrieve this machine fulfills (index into the launch-time plan's
    /// `schedule.retrieves`). Set at launch; read live by `machineFor` (is a machine
    /// running for this retrieve). Meaningful only while this machine is the live
    /// in-flight handle of that plan; the re-plan barrier (`chassert(!machine)`)
    /// guarantees none straddles a rebuild.
    size_t retrieve_index = 0;
    /// The long source connection CARRIED by this machine: moved in from the
    /// foreground at launch (a machine never opens one itself - the foreground is
    /// the sole opener), drained by the worker's fetch step instead of a one-shot
    /// GET, reclaimed by the foreground at collect, or accounted + released at reap
    /// if the machine is abandoned. Empty when the foreground carried no connection
    /// into this launch.
    std::optional<LongConnection> long_conn;
    ReaderExecutorStats stats;
    bool reached_eof = false;
    /// The fetch step's RESIDUE: raw PHYSICAL source bytes no cell accepted (a refused
    /// write, a sibling-claimed cell; a bypass window retains everything). Bytes the
    /// worker committed per tile live in the cells and are NOT held here - the serve
    /// reads them from the display. Capped at one (pressure-scaled) window by the
    /// fetch step: when nothing commits, the lead stops instead of ballooning in memory.
    ChainedBuffers fetched;
    /// The worker's PUBLISHED residue preview: a read-only copy of the tiles no
    /// cell accepted, refreshed per tile under the mutex, so the foreground
    /// display can serve them while the flight is still in progress (without it,
    /// a cacheless consumer must JOIN the machine for bytes the worker already
    /// fetched - chopping the producer at every swing of an interleaved read).
    /// The display only slice-copies - never consumes - so the preview is
    /// idempotent with the collect that delivers the same bytes to the bank,
    /// and it dies with the payload on cancel/revoke.
    std::mutex published_mutex;
    ChainedBuffers published;
    /// Signalled under the mutex after each tile's publication and once at the
    /// worker's exit (`publish_done`), so a consumer can WAIT for the next tile
    /// (the cacheless analogue of waiting on a live cell) instead of
    /// interrupting the flight.
    std::condition_variable published_cv;
    /// Worker lifecycle marks under the mutex: a consumer may only WAIT on a
    /// STARTED worker (an unstarted one may never run - a queued cancel, a
    /// stashed machine - and would never signal); `publish_done` ends the wait.
    bool publish_started = false;
    bool publish_done = false;
    /// The PHYSICAL frontier the fetch actually reached (end of the last fetched run),
    /// independent of what `fetched` retains - the pin and the attempted accounting
    /// anchor here. `physical_window.offset` when nothing was fetched.
    size_t fetched_end = 0;
    /// The fill step's targets: NON-OWNING views of the writers this fill writes
    /// (the schedule's fill targets overlapping the window). The writers stay in the
    /// shared `read_plan.tiers`; the fill runs inline on the read thread, so referencing
    /// them in place is race-free.
    VectorWithMemoryTracking<ReaderExecutorWriterView> writer_views;
    /// Set by the worker when a SIBLING is downloading some segment (this worker lost the
    /// election): it skipped fetching those, so `fetched` has holes there. At collect the
    /// foreground revokes to the synchronous path (which re-elects/waits on the sibling-led
    /// bytes on the query thread). False with no contention (the worker then leads - and
    /// fetches - the whole window).
    bool contended = false;
    /// Set when this machine is driven INLINE on the serve thread (a `LocalFetchMachineRunner`),
    /// as opposed to a pool worker reading ahead. The inline fetch "stops at the first loss":
    /// it fetches only the contiguous led PREFIX up to the first sibling-led segment, so the
    /// serve thread never blocks fetching a led run PAST a sibling-led hole (the caller's next
    /// read resolves the boundary). A pool worker keeps the full-window fetch.
    bool inline_serve = false;
    /// `ReaderExecutorPrefetchInFlight` for this machine's lifetime.
    CurrentMetrics::Increment inflight_gauge;
};

}
