#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <algorithm>
#include <iterator>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <consistent_hashing.h>

#include <IO/Progress.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/RequestResponse.h>
#include <boost/algorithm/string/split.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>


using namespace DB;

namespace
{
size_t roundDownToMultiple(size_t num, size_t multiple)
{
    return (num / multiple) * multiple;
}

size_t
takeFromRange(const MarkRange & range, size_t min_number_of_marks, size_t & current_marks_amount, RangesInDataPartDescription & result)
{
    const auto marks_needed = min_number_of_marks - current_marks_amount;
    chassert(marks_needed);
    auto range_we_take = MarkRange{range.begin, range.begin + std::min(marks_needed, range.getNumberOfMarks())};
    if (!result.ranges.empty() && result.ranges.back().end == range_we_take.begin)
        /// Can extend the previous range
        result.ranges.back().end = range_we_take.end;
    else
        result.ranges.emplace_back(range_we_take);
    current_marks_amount += range_we_take.getNumberOfMarks();
    return range_we_take.getNumberOfMarks();
}

void sortResponseRanges(RangesInDataPartsDescription & result)
{
    std::ranges::sort(result, [](const auto & lhs, const auto & rhs) { return lhs.info < rhs.info; });

    RangesInDataPartsDescription new_result;

    /// Aggregate ranges for each part within a single entry
    for (auto & ranges_in_part : result)
    {
        if (new_result.empty() || new_result.back().info != ranges_in_part.info)
            new_result.push_back(RangesInDataPartDescription{.info = ranges_in_part.info});

        new_result.back().ranges.insert(
            new_result.back().ranges.end(),
            std::make_move_iterator(ranges_in_part.ranges.begin()),
            std::make_move_iterator(ranges_in_part.ranges.end()));
        ranges_in_part.ranges.clear();
    }

    /// Sort ranges for each part
    for (auto & ranges_in_part : new_result)
        std::sort(ranges_in_part.ranges.begin(), ranges_in_part.ranges.end());

    result = std::move(new_result);
}
}

namespace ProfileEvents
{
extern const Event ParallelReplicasHandleRequestMicroseconds;
extern const Event ParallelReplicasHandleAnnouncementMicroseconds;

extern const Event ParallelReplicasStealingByHashMicroseconds;
extern const Event ParallelReplicasProcessingPartsMicroseconds;
extern const Event ParallelReplicasStealingLeftoversMicroseconds;
extern const Event ParallelReplicasCollectingOwnedSegmentsMicroseconds;

extern const Event ParallelReplicasReadAssignedMarks;
extern const Event ParallelReplicasReadUnassignedMarks;
extern const Event ParallelReplicasReadAssignedForStealingMarks;

extern const Event ParallelReplicasUsedCount;
extern const Event ParallelReplicasUnavailableCount;
}

namespace DB
{
struct Part
{
    mutable RangesInDataPartDescription description;
    // FIXME: This is needed to put this struct in set
    // and modify through iterator
    mutable std::set<size_t> replicas;

    bool operator<(const Part & rhs) const { return description.info < rhs.description.info; }
};
}

template <>
struct fmt::formatter<DB::Part>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::Part & part, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{} in replicas [{}]", part.description.describe(), fmt::join(part.replicas, ", "));
    }
};

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int ALL_CONNECTION_TRIES_FAILED;
}

class ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    struct Stat
    {
        size_t number_of_requests{0};
        size_t sum_marks{0};

        /// Marks assigned to the given replica by consistent hash
        size_t assigned_to_me = 0;
        /// Marks stolen from other replicas
        size_t stolen_unassigned = 0;

        /// Stolen marks that were assigned for stealing to the given replica by hash. Makes sense only for DefaultCoordinator
        size_t stolen_by_hash = 0;

        bool is_unavailable{false};
    };
    using Stats = std::vector<Stat>;
    static String toString(Stats stats)
    {
        String result = "Statistics: ";
        std::vector<String> stats_by_replica;
        for (size_t i = 0; i < stats.size(); ++i)
            stats_by_replica.push_back(fmt::format(
                "replica {}{} - {{requests: {} marks: {} assigned_to_me: {} stolen_by_hash: {} stolen_unassigned: {}}}",
                i,
                stats[i].is_unavailable ? " is unavailable" : "",
                stats[i].number_of_requests,
                stats[i].sum_marks,
                stats[i].assigned_to_me,
                stats[i].stolen_by_hash,
                stats[i].stolen_unassigned));
        result += fmt::format("{}", fmt::join(stats_by_replica, "; "));
        return result;
    }

    Stats stats;
    size_t replicas_count{0};
    size_t unavailable_replicas_count{0};
    size_t sent_initial_requests{0};
    ProgressCallback progress_callback;

    explicit ImplInterface(size_t replicas_count_)
        : stats{replicas_count_}
        , replicas_count(replicas_count_)
    {}

    virtual ~ImplInterface() = default;

    virtual ParallelReadResponse handleRequest(ParallelReadRequest request) = 0;
    virtual void doHandleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement) = 0;
    virtual void markReplicaAsUnavailable(size_t replica_number) = 0;

    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
    {
        if (++sent_initial_requests > replicas_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Initiator received more initial requests than there are replicas: replica_num={}", announcement.replica_num);

        doHandleInitialAllRangesAnnouncement(std::move(announcement));
    }

    void setProgressCallback(ProgressCallback callback) { progress_callback = std::move(callback); }
};

using Parts = std::set<Part>;
using PartRefs = std::deque<Parts::iterator>;


/// This coordinator relies heavily on the fact that we work with a single shard,
/// i.e. the difference in parts contained in each replica's snapshot is rather negligible (it is only recently inserted or merged parts).
/// So the guarantees we provide here are basically the same as with single-node reading: we will read from parts as their were seen by some node at the moment when query started.
///
/// Knowing that almost each part could be read by each node, we suppose ranges of each part to be available to all the replicas and thus distribute them evenly between them
/// (of course we still check if replica has access to the given part before scheduling a reading from it).
///
/// Of course we want to distribute marks evenly. Looks like it is better to split parts into reasonably small segments of equal size
/// (something between 16 and 128 granules i.e. ~100K and ~1M rows should work).
/// This approach seems to work ok for all three main cases: full scan, reading random sub-ranges and reading only {pre,suf}-fix of parts.
/// Also we could expect that more granular division will make distribution more even up to a certain point.
class DefaultCoordinator : public ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    explicit DefaultCoordinator(size_t replicas_count_)
        : ParallelReplicasReadingCoordinator::ImplInterface(replicas_count_)
        , replica_status(replicas_count_)
        , distribution_by_hash_queue(replicas_count_)
    {
    }

    ~DefaultCoordinator() override;

    ParallelReadResponse handleRequest(ParallelReadRequest request) override;

    void doHandleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement) override;

    void markReplicaAsUnavailable(size_t replica_number) override;

private:
    /// This many granules will represent a single segment of marks that will be assigned to a replica
    size_t mark_segment_size{0};

    bool state_initialized{false};
    size_t finished_replicas{0};

    struct ReplicaStatus
    {
        bool is_finished{false};
        bool is_announcement_received{false};
    };
    std::vector<ReplicaStatus> replica_status;

    LoggerPtr log = getLogger("DefaultCoordinator");

    /// Workflow of a segment:
    /// 0. `all_parts_to_read` contains all the parts and thus all the segments initially present there (virtually)
    /// 1. when we traverse `all_parts_to_read` in selectPartsAndRanges() we either:
    ///     * take this segment into output
    ///     * put this segment into `distribution_by_hash_queue` for its owner if it's available and can read from it
    ///     * otherwise put this segment into `distribution_by_hash_queue` for its stealer_by_hash if it's available and can read from it
    ///     * otherwise put this segment into `ranges_for_stealing_queue`
    /// 2. when we traverse `distribution_by_hash_queue` in `selectPartsAndRanges` we either:
    ///     * take this segment into output
    ///     * otherwise put this segment into `distribution_by_hash_queue` for its stealer_by_hash if it's available and can read from it
    ///     * otherwise put this segment into `ranges_for_stealing_queue`
    /// 3. when we figuring out that some replica is unavailable we move all segments from its `distribution_by_hash_queue` to their stealers by hash or to `ranges_for_stealing_queue`
    /// 4. when we get the announcement from a replica we move all segments it cannot read to their stealers by hash or to `ranges_for_stealing_queue`
    ///
    /// So, segments always move in one direction down this path (possibly skipping some stops):
    /// `all_parts_to_read` -> `distribution_by_hash_queue[owner]` -> `distribution_by_hash_queue[stealer_by_hash]` -> `ranges_for_stealing_queue`

    /// We take the set of parts announced by this replica as the working set for the whole query.
    /// For this replica we know for sure that
    ///     1. it sees all the parts from this set
    ///     2. it was available in the beginning of execution (since we got announcement), so if it will become unavailable at some point - query will be failed with exception.
    ///        this means that we can delegate reading of all leftover segments (i.e. segments that were not read by their owner or stealer by hash) to this node
    size_t source_replica_for_parts_snapshot{0};

    /// Parts view from the first announcement we received
    std::vector<Part> all_parts_to_read;

    std::unordered_map<std::string, std::unordered_set<size_t>> part_visibility; /// part_name -> set of replicas announced that part

    /// We order parts from biggest (= oldest) to newest and steal from newest. Because we assume
    /// that they're gonna be merged soon anyway and for them we should already expect worse cache hit.
    struct BiggerPartsFirst
    {
        bool operator()(const auto & lhs, const auto & rhs) const { return lhs.info.getBlocksCount() > rhs.info.getBlocksCount(); }
    };

    /// We don't precalculate the whole assignment for each node at the start.
    /// When replica asks coordinator for a new portion of data to read, it traverses `all_parts_to_read` to find ranges relevant to this replica (by consistent hash).
    /// Many hashes are being calculated during this process and just to not loose this time we save the information about all these ranges
    /// observed along the way to what node they belong to.
    /// Ranges in this queue might belong to a part that the given replica cannot read from - the corresponding check happens later.
    /// TODO: consider making it bounded in size
    std::vector<std::multiset<RangesInDataPartDescription, BiggerPartsFirst>> distribution_by_hash_queue;

    /// For some ranges their owner and stealer (by consistent hash) cannot read from the given part at all. So this range have to be stolen anyway.
    /// TODO: consider making it bounded in size
    RangesInDataPartsDescription ranges_for_stealing_queue;

    /// We take only first replica's set of parts as the whole working set for this query.
    /// For other replicas we'll just discard parts that they know, but that weren't present in the first request we received.
    /// The second and all subsequent announcements needed only to understand if we can schedule reading from the given part to the given replica.
    void initializeReadingState(InitialAllRangesAnnouncement announcement);

    void setProgressCallback();

    enum class ScanMode : uint8_t
    {
        /// Main working set for the replica
        TakeWhatsMineByHash,
        /// We need to steal to optimize tail latency, let's do it by hash nevertheless
        TakeWhatsMineForStealing,
        /// All bets are off, we need to steal "for correctness" - to not leave any segments unread
        TakeEverythingAvailable
    };

    void selectPartsAndRanges(
        size_t replica_num,
        ScanMode scan_mode,
        size_t min_number_of_marks,
        size_t & current_marks_amount,
        RangesInDataPartsDescription & description);

    size_t computeConsistentHash(const std::string & part_name, size_t segment_begin, ScanMode scan_mode) const;

    void tryToTakeFromDistributionQueue(
        size_t replica_num, size_t min_number_of_marks, size_t & current_marks_amount, RangesInDataPartsDescription & description);

    void tryToStealFromQueues(
        size_t replica_num,
        ScanMode scan_mode,
        size_t min_number_of_marks,
        size_t & current_marks_amount,
        RangesInDataPartsDescription & description);

    void tryToStealFromQueue(
        auto & queue,
        ssize_t owner, /// In case `queue` is `distribution_by_hash_queue[replica]`
        size_t replica_num,
        ScanMode scan_mode,
        size_t min_number_of_marks,
        size_t & current_marks_amount,
        RangesInDataPartsDescription & description);

    void processPartsFurther(
        size_t replica_num,
        ScanMode scan_mode,
        size_t min_number_of_marks,
        size_t & current_marks_amount,
        RangesInDataPartsDescription & description);

    bool possiblyCanReadPart(size_t replica, const MergeTreePartInfo & info) const;
    void enqueueSegment(const MergeTreePartInfo & info, const MarkRange & segment, size_t owner);
    void enqueueToStealerOrStealingQueue(const MergeTreePartInfo & info, const MarkRange & segment);
};


DefaultCoordinator::~DefaultCoordinator()
{
    try
    {
        LOG_DEBUG(log, "Coordination done: {}", toString(stats));
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void DefaultCoordinator::initializeReadingState(InitialAllRangesAnnouncement announcement)
{
    for (const auto & part : announcement.description)
    {
        /// We don't really care here if this part will be included into the working set or not
        part_visibility[part.info.getPartNameV1()].insert(announcement.replica_num);
    }

    /// If state is already initialized - just register availabitily info and leave
    if (state_initialized)
        return;

    {
        /// To speedup search for adjacent parts
        Parts known_parts(all_parts_to_read.begin(), all_parts_to_read.end());

        for (auto && part : announcement.description)
        {
            auto intersecting_it = known_parts.lower_bound(Part{.description = part, .replicas = {}});

            if (intersecting_it != known_parts.end() && !intersecting_it->description.info.isDisjoint(part.info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intersecting parts found in announcement");

            known_parts.emplace(Part{.description = part, .replicas = {}});
            all_parts_to_read.push_back(Part{.description = std::move(part), .replicas = {announcement.replica_num}});
        }
    }

    std::ranges::sort(
        all_parts_to_read, [](const Part & lhs, const Part & rhs) { return BiggerPartsFirst()(lhs.description, rhs.description); });
    state_initialized = true;
    source_replica_for_parts_snapshot = announcement.replica_num;

    mark_segment_size = announcement.mark_segment_size;
    if (mark_segment_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Zero value provided for `mark_segment_size`");

    LOG_DEBUG(log, "Reading state is fully initialized: {}, mark_segment_size: {}", fmt::join(all_parts_to_read, "; "), mark_segment_size);
}

void DefaultCoordinator::markReplicaAsUnavailable(size_t replica_number)
{
    LOG_DEBUG(log, "Replica number {} is unavailable", replica_number);

    ++unavailable_replicas_count;
    stats[replica_number].is_unavailable = true;

    if (sent_initial_requests == replicas_count - unavailable_replicas_count)
        setProgressCallback();

    for (const auto & segment : distribution_by_hash_queue[replica_number])
    {
        chassert(segment.ranges.size() == 1);
        enqueueToStealerOrStealingQueue(segment.info, segment.ranges.front());
    }
    distribution_by_hash_queue[replica_number].clear();
}

void DefaultCoordinator::setProgressCallback()
{
    // Update progress with total rows
    if (progress_callback)
    {
        size_t total_rows_to_read = 0;
        for (const auto & part : all_parts_to_read)
            total_rows_to_read += part.description.rows;

        Progress progress;
        progress.total_rows_to_read = total_rows_to_read;
        progress_callback(progress);

        LOG_DEBUG(log, "Total rows to read: {}", total_rows_to_read);
    }
}

void DefaultCoordinator::doHandleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    LOG_DEBUG(log, "Initial request: {}", announcement.describe());

    const auto replica_num = announcement.replica_num;

    initializeReadingState(std::move(announcement));

    if (replica_num >= stats.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Replica number ({}) is bigger than total replicas count ({})", replica_num, stats.size());

    ++stats[replica_num].number_of_requests;

    if (replica_status[replica_num].is_announcement_received)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate announcement received for replica number {}", replica_num);
    replica_status[replica_num].is_announcement_received = true;

    LOG_DEBUG(log, "Sent initial requests: {} Replicas count: {}", sent_initial_requests, replicas_count);

    if (sent_initial_requests == replicas_count - unavailable_replicas_count)
        setProgressCallback();

    /// Sift the queue to move out all invisible segments
    for (auto segment_it = distribution_by_hash_queue[replica_num].begin(); segment_it != distribution_by_hash_queue[replica_num].end();)
    {
        if (!part_visibility[segment_it->info.getPartNameV1()].contains(replica_num))
        {
            chassert(segment_it->ranges.size() == 1);
            enqueueToStealerOrStealingQueue(segment_it->info, segment_it->ranges.front());
            segment_it = distribution_by_hash_queue[replica_num].erase(segment_it);
        }
        else
        {
            ++segment_it;
        }
    }
}

void DefaultCoordinator::tryToTakeFromDistributionQueue(
    size_t replica_num, size_t min_number_of_marks, size_t & current_marks_amount, RangesInDataPartsDescription & description)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasCollectingOwnedSegmentsMicroseconds);

    auto & distribution_queue = distribution_by_hash_queue[replica_num];
    auto replica_can_read_part = [&](auto replica, const auto & part) { return part_visibility[part.getPartNameV1()].contains(replica); };

    RangesInDataPartDescription result;

    while (!distribution_queue.empty() && current_marks_amount < min_number_of_marks)
    {
        if (result.ranges.empty() || distribution_queue.begin()->info != result.info)
        {
            if (!result.ranges.empty())
                /// We're switching to a different part, so have to save currently accumulated ranges
                description.push_back(result);
            result = {.info = distribution_queue.begin()->info};
        }

        /// NOTE: this works because ranges are not considered by the comparator
        auto & part_ranges = const_cast<RangesInDataPartDescription &>(*distribution_queue.begin());
        chassert(part_ranges.ranges.size() == 1);
        auto & range = part_ranges.ranges.front();

        if (replica_can_read_part(replica_num, part_ranges.info))
        {
            if (auto taken = takeFromRange(range, min_number_of_marks, current_marks_amount, result); taken == range.getNumberOfMarks())
                distribution_queue.erase(distribution_queue.begin());
            else
            {
                range.begin += taken;
                break;
            }
        }
        else
        {
            /// It might be that `replica_num` is the stealer by hash itself - no problem,
            /// we'll just have a redundant hash computation inside this function
            enqueueToStealerOrStealingQueue(part_ranges.info, range);
            distribution_queue.erase(distribution_queue.begin());
        }
    }

    if (!result.ranges.empty())
        description.push_back(result);
}

void DefaultCoordinator::tryToStealFromQueues(
    size_t replica_num,
    ScanMode scan_mode,
    size_t min_number_of_marks,
    size_t & current_marks_amount,
    RangesInDataPartsDescription & description)
{
    auto steal_from_other_replicas = [&]()
    {
        /// Try to steal from other replicas starting from replicas with longest queues
        std::vector<size_t> order(replicas_count);
        std::iota(order.begin(), order.end(), 0);
        std::ranges::sort(
            order, [&](auto lhs, auto rhs) { return distribution_by_hash_queue[lhs].size() > distribution_by_hash_queue[rhs].size(); });

        for (auto replica : order)
            tryToStealFromQueue(
                distribution_by_hash_queue[replica],
                replica,
                replica_num,
                scan_mode,
                min_number_of_marks,
                current_marks_amount,
                description);
    };

    if (scan_mode == ScanMode::TakeWhatsMineForStealing)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasStealingByHashMicroseconds);
        steal_from_other_replicas();
    }
    else
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasStealingLeftoversMicroseconds);
        /// Check orphaned ranges
        tryToStealFromQueue(
            ranges_for_stealing_queue, /*owner=*/-1, replica_num, scan_mode, min_number_of_marks, current_marks_amount, description);
        /// Last hope. In case we haven't yet figured out that some node is unavailable its segments are still in the distribution queue.
        steal_from_other_replicas();
    }
}

void DefaultCoordinator::tryToStealFromQueue(
    auto & queue,
    ssize_t owner,
    size_t replica_num,
    ScanMode scan_mode,
    size_t min_number_of_marks,
    size_t & current_marks_amount,
    RangesInDataPartsDescription & description)
{
    auto replica_can_read_part = [&](auto replica, const auto & part) { return part_visibility[part.getPartNameV1()].contains(replica); };

    RangesInDataPartDescription result;

    auto it = queue.rbegin();
    while (it != queue.rend() && current_marks_amount < min_number_of_marks)
    {
        auto & part_ranges = const_cast<RangesInDataPartDescription &>(*it);
        chassert(part_ranges.ranges.size() == 1);
        auto & range = part_ranges.ranges.front();

        if (result.ranges.empty() || part_ranges.info != result.info)
        {
            if (!result.ranges.empty())
                /// We're switching to a different part, so have to save currently accumulated ranges
                description.push_back(result);
            result = {.info = part_ranges.info};
        }

        if (replica_can_read_part(replica_num, part_ranges.info))
        {
            bool can_take = false;
            if (scan_mode == ScanMode::TakeWhatsMineForStealing)
            {
                chassert(owner >= 0);
                const size_t segment_begin = roundDownToMultiple(range.begin, mark_segment_size);
                can_take = computeConsistentHash(part_ranges.info.getPartNameV1(), segment_begin, scan_mode) == replica_num;
            }
            else
            {
                /// Don't steal segments with alive owner that sees them
                can_take = owner == -1 || stats[owner].is_unavailable || !replica_status[owner].is_announcement_received;
            }
            if (can_take)
            {
                if (auto taken = takeFromRange(range, min_number_of_marks, current_marks_amount, result); taken == range.getNumberOfMarks())
                {
                    it = decltype(it)(queue.erase(std::next(it).base()));
                    continue;
                }
                else
                    range.begin += taken;
            }
        }

        ++it;
    }

    if (!result.ranges.empty())
        description.push_back(result);
}

void DefaultCoordinator::processPartsFurther(
    size_t replica_num,
    ScanMode scan_mode,
    size_t min_number_of_marks,
    size_t & current_marks_amount,
    RangesInDataPartsDescription & description)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasProcessingPartsMicroseconds);

    auto replica_can_read_part = [&](auto replica, const auto & part) { return part_visibility[part.getPartNameV1()].contains(replica); };

    for (const auto & part : all_parts_to_read)
    {
        if (current_marks_amount >= min_number_of_marks)
        {
            LOG_TEST(log, "Current mark size {} is bigger than min_number_marks {}", current_marks_amount, min_number_of_marks);
            return;
        }

        RangesInDataPartDescription result{.info = part.description.info};

        while (!part.description.ranges.empty() && current_marks_amount < min_number_of_marks)
        {
            auto & range = part.description.ranges.front();

            /// Parts are divided into segments of `mark_segment_size` granules staring from 0-th granule
            for (size_t segment_begin = roundDownToMultiple(range.begin, mark_segment_size);
                 segment_begin < range.end && current_marks_amount < min_number_of_marks;
                 segment_begin += mark_segment_size)
            {
                const auto cur_segment
                    = MarkRange{std::max(range.begin, segment_begin), std::min(range.end, segment_begin + mark_segment_size)};

                const auto owner = computeConsistentHash(part.description.info.getPartNameV1(), segment_begin, scan_mode);
                if (owner == replica_num && replica_can_read_part(replica_num, part.description.info))
                {
                    const auto taken = takeFromRange(cur_segment, min_number_of_marks, current_marks_amount, result);
                    if (taken == range.getNumberOfMarks())
                        part.description.ranges.pop_front();
                    else
                    {
                        range.begin += taken;
                        break;
                    }
                }
                else
                {
                    chassert(scan_mode == ScanMode::TakeWhatsMineByHash);
                    enqueueSegment(part.description.info, cur_segment, owner);
                    range.begin += cur_segment.getNumberOfMarks();
                    if (range.getNumberOfMarks() == 0)
                        part.description.ranges.pop_front();
                }
            }
        }

        if (!result.ranges.empty())
            description.push_back(std::move(result));
    }
}

void DefaultCoordinator::selectPartsAndRanges(
    size_t replica_num,
    ScanMode scan_mode,
    size_t min_number_of_marks,
    size_t & current_marks_amount,
    RangesInDataPartsDescription & description)
{
    if (scan_mode == ScanMode::TakeWhatsMineByHash)
    {
        tryToTakeFromDistributionQueue(replica_num, min_number_of_marks, current_marks_amount, description);
        processPartsFurther(replica_num, scan_mode, min_number_of_marks, current_marks_amount, description);
        /// We might back-fill `distribution_by_hash_queue` for this replica in `enqueueToStealerOrStealingQueue`
        tryToTakeFromDistributionQueue(replica_num, min_number_of_marks, current_marks_amount, description);
    }
    else
        tryToStealFromQueues(replica_num, scan_mode, min_number_of_marks, current_marks_amount, description);
}

bool DefaultCoordinator::possiblyCanReadPart(size_t replica, const MergeTreePartInfo & info) const
{
    /// At this point we might not be sure if `owner` can read from the given part.
    /// Then we will check it while processing `owner`'s data requests - they are guaranteed to came after the announcement.
    return !stats[replica].is_unavailable && !replica_status[replica].is_finished
        && (!replica_status[replica].is_announcement_received || part_visibility.at(info.getPartNameV1()).contains(replica));
}

void DefaultCoordinator::enqueueSegment(const MergeTreePartInfo & info, const MarkRange & segment, size_t owner)
{
    if (possiblyCanReadPart(owner, info))
    {
        /// TODO: optimize me (maybe we can store something lighter than RangesInDataPartDescription)
        distribution_by_hash_queue[owner].insert(RangesInDataPartDescription{.info = info, .ranges = {segment}});
        LOG_TEST(log, "Segment {} of {} is added to its owner's ({}) queue", segment, info.getPartNameV1(), owner);
    }
    else
        enqueueToStealerOrStealingQueue(info, segment);
}

void DefaultCoordinator::enqueueToStealerOrStealingQueue(const MergeTreePartInfo & info, const MarkRange & segment)
{
    auto && range = RangesInDataPartDescription{.info = info, .ranges = {segment}};
    const auto stealer_by_hash = computeConsistentHash(
        info.getPartNameV1(), roundDownToMultiple(segment.begin, mark_segment_size), ScanMode::TakeWhatsMineForStealing);
    if (possiblyCanReadPart(stealer_by_hash, info))
    {
        distribution_by_hash_queue[stealer_by_hash].insert(std::move(range));
        LOG_TEST(log, "Segment {} of {} is added to its stealer's ({}) queue", segment, info.getPartNameV1(), stealer_by_hash);
    }
    else
    {
        ranges_for_stealing_queue.push_back(std::move(range));
        LOG_TEST(log, "Segment {} of {} is added to stealing queue", segment, info.getPartNameV1());
    }
}

size_t DefaultCoordinator::computeConsistentHash(const std::string & part_name, size_t segment_begin, ScanMode scan_mode) const
{
    chassert(segment_begin % mark_segment_size == 0);
    auto hash = SipHash();
    hash.update(part_name);
    hash.update(segment_begin);
    hash.update(scan_mode);
    return ConsistentHashing(hash.get64(), replicas_count);
}

ParallelReadResponse DefaultCoordinator::handleRequest(ParallelReadRequest request)
{
    LOG_TRACE(log, "Handling request from replica {}, minimal marks size is {}", request.replica_num, request.min_number_of_marks);

    ParallelReadResponse response;

    size_t current_mark_size = 0;

    /// 1. Try to select ranges meant for this replica by consistent hash
    selectPartsAndRanges(
        request.replica_num, ScanMode::TakeWhatsMineByHash, request.min_number_of_marks, current_mark_size, response.description);
    const size_t assigned_to_me = current_mark_size;

    /// 2. Try to steal but with caching again (with different key)
    selectPartsAndRanges(
        request.replica_num, ScanMode::TakeWhatsMineForStealing, request.min_number_of_marks, current_mark_size, response.description);
    const size_t stolen_by_hash = current_mark_size - assigned_to_me;

    /// 3. Try to steal with no preference. We're trying to postpone it as much as possible.
    if (current_mark_size == 0 && request.replica_num == source_replica_for_parts_snapshot)
        selectPartsAndRanges(
            request.replica_num, ScanMode::TakeEverythingAvailable, request.min_number_of_marks, current_mark_size, response.description);
    const size_t stolen_unassigned = current_mark_size - stolen_by_hash - assigned_to_me;

    stats[request.replica_num].number_of_requests += 1;
    stats[request.replica_num].sum_marks += current_mark_size;

    stats[request.replica_num].assigned_to_me += assigned_to_me;
    stats[request.replica_num].stolen_by_hash += stolen_by_hash;
    stats[request.replica_num].stolen_unassigned += stolen_unassigned;

    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadAssignedMarks, assigned_to_me);
    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadUnassignedMarks, stolen_unassigned);
    ProfileEvents::increment(ProfileEvents::ParallelReplicasReadAssignedForStealingMarks, stolen_by_hash);

    if (response.description.empty())
    {
        response.finish = true;

        replica_status[request.replica_num].is_finished = true;

        if (++finished_replicas == replicas_count - unavailable_replicas_count)
        {
            /// Nobody will come to process any more data

            for (const auto & part : all_parts_to_read)
                if (!part.description.ranges.empty())
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Some segments were left unread for the part {}", part.description.describe());

            if (!ranges_for_stealing_queue.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Some orphaned segments were left unread");

            for (size_t replica = 0; replica < replicas_count; ++replica)
                if (!distribution_by_hash_queue[replica].empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty distribution_by_hash_queue for replica {}", replica);
        }
    }

    sortResponseRanges(response.description);

    LOG_DEBUG(
        log,
        "Going to respond to replica {} with {}; mine_marks={}, stolen_by_hash={}, stolen_rest={}",
        request.replica_num,
        response.describe(),
        assigned_to_me,
        stolen_by_hash,
        stolen_unassigned);

    return response;
}


template <CoordinationMode mode>
class InOrderCoordinator : public ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    explicit InOrderCoordinator([[ maybe_unused ]] size_t replicas_count_)
        : ParallelReplicasReadingCoordinator::ImplInterface(replicas_count_)
    {}
    ~InOrderCoordinator() override
    {
        LOG_DEBUG(log, "Coordination done: {}", toString(stats));
    }

    ParallelReadResponse handleRequest([[ maybe_unused ]]  ParallelReadRequest request) override;
    void doHandleInitialAllRangesAnnouncement([[maybe_unused]] InitialAllRangesAnnouncement announcement) override;
    void markReplicaAsUnavailable(size_t replica_number) override;

    Parts all_parts_to_read;
    size_t total_rows_to_read = 0;
    bool state_initialized{false};

    LoggerPtr log = getLogger(fmt::format("{}{}", magic_enum::enum_name(mode), "Coordinator"));
};

template <CoordinationMode mode>
void InOrderCoordinator<mode>::markReplicaAsUnavailable(size_t replica_number)
{
    if (stats[replica_number].is_unavailable == false)
    {
        LOG_DEBUG(log, "Replica number {} is unavailable", replica_number);

        stats[replica_number].is_unavailable = true;
        ++unavailable_replicas_count;
    }
}

template <CoordinationMode mode>
void InOrderCoordinator<mode>::doHandleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    LOG_TRACE(log, "Received an announcement : {}", announcement.describe());

    ++stats[announcement.replica_num].number_of_requests;

    size_t new_rows_to_read = 0;

    /// To get rid of duplicates
    for (auto && part: announcement.description)
    {
        auto the_same_it = all_parts_to_read.find(Part{.description = part, .replicas = {}});

        /// We have the same part - add the info about presence on the corresponding replica to it
        if (the_same_it != all_parts_to_read.end())
        {
            the_same_it->replicas.insert(announcement.replica_num);
            continue;
        }

        if (state_initialized)
            continue;

        /// Look for the first part >= current
        auto covering_it = all_parts_to_read.lower_bound(Part{.description = part, .replicas = {}});

        if (covering_it != all_parts_to_read.end())
        {
            /// Checks if other part covers this one or this one covers the other
            auto is_covered_or_covering = [&part] (const Part & other)
                {
                    return other.description.info.contains(part.info) || part.info.contains(other.description.info);
                };

            if (is_covered_or_covering(*covering_it))
                continue;

            /// Also look at the previous part, it could be covering the current one
            if (covering_it != all_parts_to_read.begin())
            {
                --covering_it;
                if (is_covered_or_covering(*covering_it))
                    continue;
            }
        }

        new_rows_to_read += part.rows;

        auto [inserted_it, _] = all_parts_to_read.emplace(Part{.description = std::move(part), .replicas = {announcement.replica_num}});
        auto & ranges = inserted_it->description.ranges;
        std::sort(ranges.begin(), ranges.end());
    }

#ifndef NDEBUG
    /// Double check that there are no intersecting parts
    {
        auto intersecting_part_it = std::adjacent_find(all_parts_to_read.begin(), all_parts_to_read.end(),
            [] (const Part & lhs, const Part & rhs)
            {
                return !lhs.description.info.isDisjoint(rhs.description.info);
            });

        if (intersecting_part_it != all_parts_to_read.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Parts {} and {} intersect",
                intersecting_part_it->description.info.getPartNameV1(), std::next(intersecting_part_it)->description.info.getPartNameV1());
    }
#endif

    state_initialized = true;

    // progress_callback is not set when local plan is used for initiator
    if (progress_callback && new_rows_to_read > 0)
    {
        Progress progress;
        progress.total_rows_to_read = new_rows_to_read;
        progress_callback(progress);

        total_rows_to_read += new_rows_to_read;

        LOG_DEBUG(log, "Updated total rows to read: added {} rows, total {} rows", new_rows_to_read, total_rows_to_read);
    }
}

template <CoordinationMode mode>
ParallelReadResponse InOrderCoordinator<mode>::handleRequest(ParallelReadRequest request)
{
    if (request.mode != mode)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Replica {} decided to read in {} mode, not in {}. This is a bug",
            request.replica_num, magic_enum::enum_name(request.mode), magic_enum::enum_name(mode));

    LOG_TRACE(log, "Got read request: {}", request.describe());

    ParallelReadResponse response;
    response.description = request.description;
    size_t overall_number_of_marks = 0;

    for (auto & part : response.description)
    {
        auto global_part_it = std::find_if(all_parts_to_read.begin(), all_parts_to_read.end(),
            [&part] (const Part & other) { return other.description.info == part.info; });

        if (global_part_it == all_parts_to_read.end())
            continue;

        if (global_part_it->replicas.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Part {} requested by replica {} is not registered in working set",
                part.info.getPartNameV1(),
                request.replica_num);

        if (!global_part_it->replicas.contains(request.replica_num))
            continue;

        size_t current_mark_size = 0;

        /// Now we can recommend to read more intervals
        if constexpr (mode == CoordinationMode::ReverseOrder)
        {
            while (!global_part_it->description.ranges.empty() && current_mark_size < request.min_number_of_marks)
            {
                auto & range = global_part_it->description.ranges.back();
                const size_t needed = request.min_number_of_marks - current_mark_size;

                if (range.getNumberOfMarks() > needed)
                {
                    auto range_we_take = MarkRange{range.end - needed, range.end};
                    part.ranges.emplace_front(range_we_take);
                    current_mark_size += range_we_take.getNumberOfMarks();

                    range.end -= needed;
                    break;
                }

                part.ranges.emplace_front(range);
                current_mark_size += range.getNumberOfMarks();
                global_part_it->description.ranges.pop_back();
            }
        }
        else if constexpr (mode == CoordinationMode::WithOrder)
        {
            while (!global_part_it->description.ranges.empty() && current_mark_size < request.min_number_of_marks)
            {
                auto & range = global_part_it->description.ranges.front();
                const size_t needed = request.min_number_of_marks - current_mark_size;

                if (range.getNumberOfMarks() > needed)
                {
                    auto range_we_take = MarkRange{range.begin, range.begin + needed};
                    part.ranges.emplace_back(range_we_take);
                    current_mark_size += range_we_take.getNumberOfMarks();

                    range.begin += needed;
                    break;
                }

                part.ranges.emplace_back(range);
                current_mark_size += range.getNumberOfMarks();
                global_part_it->description.ranges.pop_front();
            }
        }

        overall_number_of_marks += current_mark_size;
    }

    if (!overall_number_of_marks)
        response.finish = true;

    stats[request.replica_num].number_of_requests += 1;
    stats[request.replica_num].sum_marks += overall_number_of_marks;

    LOG_TRACE(log, "Going to respond to replica {} with {}", request.replica_num, response.describe());
    return response;
}


void ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasHandleAnnouncementMicroseconds);

    std::lock_guard lock(mutex);

    if (!pimpl)
        initialize(announcement.mode);

    pimpl->handleInitialAllRangesAnnouncement(std::move(announcement));
}

ParallelReadResponse ParallelReplicasReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    if (request.min_number_of_marks == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero (likely because of weird interference of settings)");

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::ParallelReplicasHandleRequestMicroseconds);

    std::lock_guard lock(mutex);

    if (!pimpl)
        initialize(request.mode);

    const auto replica_num = request.replica_num;
    auto response = pimpl->handleRequest(std::move(request));
    if (!response.finish)
    {
        if (replicas_used.insert(replica_num).second)
            ProfileEvents::increment(ProfileEvents::ParallelReplicasUsedCount);
    }

    return response;
}

void ParallelReplicasReadingCoordinator::markReplicaAsUnavailable(size_t replica_number)
{
    ProfileEvents::increment(ProfileEvents::ParallelReplicasUnavailableCount);

    std::lock_guard lock(mutex);

    if (!pimpl)
    {
        unavailable_nodes_registered_before_initialization.push_back(replica_number);
        if (unavailable_nodes_registered_before_initialization.size() == replicas_count)
            throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Can't connect to any replica chosen for query execution");
    }
    else
        pimpl->markReplicaAsUnavailable(replica_number);
}

void ParallelReplicasReadingCoordinator::initialize(CoordinationMode mode)
{
    switch (mode)
    {
        case CoordinationMode::Default:
            pimpl = std::make_unique<DefaultCoordinator>(replicas_count);
            break;
        case CoordinationMode::WithOrder:
            pimpl = std::make_unique<InOrderCoordinator<CoordinationMode::WithOrder>>(replicas_count);
            break;
        case CoordinationMode::ReverseOrder:
            pimpl = std::make_unique<InOrderCoordinator<CoordinationMode::ReverseOrder>>(replicas_count);
            break;
    }

    // progress_callback is not set when local plan is used for initiator
    if (progress_callback)
        pimpl->setProgressCallback(std::move(progress_callback));

    for (const auto replica : unavailable_nodes_registered_before_initialization)
        pimpl->markReplicaAsUnavailable(replica);
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator(size_t replicas_count_) : replicas_count(replicas_count_)
{
}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

void ParallelReplicasReadingCoordinator::setProgressCallback(ProgressCallback callback)
{
    // store callback since pimpl can be not instantiated yet
    progress_callback = std::move(callback);
    if (pimpl)
        pimpl->setProgressCallback(std::move(progress_callback));
}

}
