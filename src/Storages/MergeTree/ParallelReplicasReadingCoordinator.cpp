#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <algorithm>
#include <mutex>
#include <numeric>
#include <vector>
#include <map>
#include <set>

#include <consistent_hashing.h>

#include "Common/Exception.h"
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>
#include <base/types.h>
#include "IO/WriteBufferFromString.h"
#include <IO/Progress.h>
#include "Storages/MergeTree/RangesInDataPart.h"
#include "Storages/MergeTree/RequestResponse.h"
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>
#include <fmt/core.h>
#include <fmt/format.h>

namespace ProfileEvents
{
    extern const Event ParallelReplicasUsedCount;
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
    auto format(const DB::Part & part, FormatContext & ctx)
    {
        return fmt::format_to(ctx.out(), "{} in replicas [{}]", part.description.describe(), fmt::join(part.replicas, ", "));
    }
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    struct Stat
    {
        size_t number_of_requests{0};
        size_t sum_marks{0};
        bool is_unavailable{false};
    };
    using Stats = std::vector<Stat>;
    static String toString(Stats stats)
    {
        String result = "Statistics: ";
        std::vector<String> stats_by_replica;
        for (size_t i = 0; i < stats.size(); ++i)
            stats_by_replica.push_back(fmt::format("replica {}{} - {{requests: {} marks: {}}}", i, stats[i].is_unavailable ? " is unavailable" : "", stats[i].number_of_requests, stats[i].sum_marks));
        result += fmt::format("{}", fmt::join(stats_by_replica, "; "));
        return result;
    }

    Stats stats;
    size_t replicas_count{0};
    size_t unavailable_replicas_count{0};
    ProgressCallback progress_callback;

    explicit ImplInterface(size_t replicas_count_)
        : stats{replicas_count_}
        , replicas_count(replicas_count_)
    {}

    virtual ~ImplInterface() = default;
    virtual ParallelReadResponse handleRequest(ParallelReadRequest request) = 0;
    virtual void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement) = 0;
    virtual void markReplicaAsUnavailable(size_t replica_number) = 0;

    void setProgressCallback(ProgressCallback callback) { progress_callback = std::move(callback); }
};

using Parts = std::set<Part>;
using PartRefs = std::deque<Parts::iterator>;


class DefaultCoordinator : public ParallelReplicasReadingCoordinator::ImplInterface
{
public:
    using ParallelReadRequestPtr = std::unique_ptr<ParallelReadRequest>;
    using PartToMarkRanges = std::map<PartToRead::PartAndProjectionNames, HalfIntervals>;

    explicit DefaultCoordinator(size_t replicas_count_)
        : ParallelReplicasReadingCoordinator::ImplInterface(replicas_count_)
        , reading_state(replicas_count_)
    {
    }

    ~DefaultCoordinator() override;

    struct PartitionReading
    {
        PartSegments part_ranges;
        PartToMarkRanges mark_ranges_in_part;
    };

    using PartitionToBlockRanges = std::map<String, PartitionReading>;
    PartitionToBlockRanges partitions;

    size_t sent_initial_requests{0};

    Parts all_parts_to_read;
    /// Contains only parts which we haven't started to read from
    PartRefs delayed_parts;
    /// Per-replica preferred parts split by consistent hash
    /// Once all task will be done by some replica, it can steal tasks
    std::vector<PartRefs> reading_state;

    Poco::Logger * log = &Poco::Logger::get("DefaultCoordinator");

    std::atomic<bool> state_initialized{false};

    ParallelReadResponse handleRequest(ParallelReadRequest request) override;
    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement) override;
    void markReplicaAsUnavailable(size_t replica_number) override;

    void updateReadingState(InitialAllRangesAnnouncement announcement);
    void finalizeReadingState();

    size_t computeConsistentHash(const MergeTreePartInfo & info) const
    {
        auto hash = SipHash();
        hash.update(info.getPartNameV1());
        return ConsistentHashing(hash.get64(), replicas_count);
    }

    void selectPartsAndRanges(const PartRefs & container, size_t replica_num, size_t min_number_of_marks, size_t & current_mark_size, ParallelReadResponse & response) const;
};

DefaultCoordinator::~DefaultCoordinator()
{
    LOG_DEBUG(log, "Coordination done: {}", toString(stats));
}

void DefaultCoordinator::updateReadingState(InitialAllRangesAnnouncement announcement)
{
    PartRefs parts_diff;

    /// To get rid of duplicates
    for (auto && part_ranges: announcement.description)
    {
        Part part{.description = std::move(part_ranges), .replicas = {announcement.replica_num}};
        const MergeTreePartInfo & announced_part = part.description.info;

        auto it = std::lower_bound(cbegin(all_parts_to_read), cend(all_parts_to_read), part);
        if (it != all_parts_to_read.cend())
        {
            const MergeTreePartInfo & found_part = it->description.info;
            if (found_part == announced_part)
            {
                /// We have the same part - add the info about presence on current replica
                it->replicas.insert(announcement.replica_num);
                continue;
            }
            else
            {
                /// check if it is covering or covered part
                /// need to compare with 2 nearest parts in set, - lesser and greater than the part from the announcement
                bool is_disjoint = found_part.isDisjoint(announced_part);
                if (it != all_parts_to_read.cbegin() && is_disjoint)
                {
                    const MergeTreePartInfo & lesser_part = (--it)->description.info;
                    is_disjoint &= lesser_part.isDisjoint(announced_part);
                }
                if (!is_disjoint)
                    continue;
            }
        }
        else if (!all_parts_to_read.empty())
        {
            /// the announced part is greatest - check if it's disjoint with lesser part
            const MergeTreePartInfo & lesser_part = all_parts_to_read.crbegin()->description.info;
            if (!lesser_part.isDisjoint(announced_part))
                continue;
        }

        auto [insert_it, _] = all_parts_to_read.emplace(std::move(part));
        parts_diff.push_back(insert_it);
    }

    /// Split all parts by consistent hash
    while (!parts_diff.empty())
    {
        auto current_part_it = parts_diff.front();
        parts_diff.pop_front();
        auto consistent_hash = computeConsistentHash(current_part_it->description.info);

        /// Check whether the new part can easy go to replica queue
        if (current_part_it->replicas.contains(consistent_hash))
        {
            reading_state[consistent_hash].emplace_back(current_part_it);
            continue;
        }

        /// Add to delayed parts
        delayed_parts.emplace_back(current_part_it);
    }
}

void DefaultCoordinator::markReplicaAsUnavailable(size_t replica_number)
{
    if (stats[replica_number].is_unavailable == false)
    {
        LOG_DEBUG(log, "Replica number {} is unavailable", replica_number);

        stats[replica_number].is_unavailable = true;
        ++unavailable_replicas_count;

        if (sent_initial_requests == replicas_count - unavailable_replicas_count)
            finalizeReadingState();
    }
}

void DefaultCoordinator::finalizeReadingState()
{
    /// Clear all the delayed queue
    while (!delayed_parts.empty())
    {
        auto current_part_it = delayed_parts.front();
        auto consistent_hash = computeConsistentHash(current_part_it->description.info);

        if (current_part_it->replicas.contains(consistent_hash))
        {
            reading_state[consistent_hash].emplace_back(current_part_it);
            delayed_parts.pop_front();
            continue;
        }

        /// In this situation just assign to a random replica which has this part
        auto replica = *(std::next(current_part_it->replicas.begin(), thread_local_rng() % current_part_it->replicas.size()));
        reading_state[replica].emplace_back(current_part_it);
        delayed_parts.pop_front();
    }

    // update progress with total rows
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

    LOG_DEBUG(log, "Reading state is fully initialized: {}", fmt::join(all_parts_to_read, "; "));
}


void DefaultCoordinator::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    const auto replica_num = announcement.replica_num;

    updateReadingState(std::move(announcement));

    if (replica_num >= stats.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Replica number ({}) is bigger than total replicas count ({})", replica_num, stats.size());

    ++stats[replica_num].number_of_requests;

    ++sent_initial_requests;
    LOG_DEBUG(log, "Sent initial requests: {} Replicas count: {}", sent_initial_requests, replicas_count);
    if (sent_initial_requests == replicas_count)
        finalizeReadingState();
}

void DefaultCoordinator::selectPartsAndRanges(const PartRefs & container, size_t replica_num, size_t min_number_of_marks, size_t & current_mark_size, ParallelReadResponse & response) const
{
    for (const auto & part : container)
    {
        if (current_mark_size >= min_number_of_marks)
        {
            LOG_TEST(log, "Current mark size {} is bigger than min_number_marks {}", current_mark_size, min_number_of_marks);
            break;
        }

        if (part->description.ranges.empty())
        {
            LOG_TEST(log, "Part {} is already empty in reading state", part->description.info.getPartNameV1());
            continue;
        }

        if (std::find(part->replicas.begin(), part->replicas.end(), replica_num) == part->replicas.end())
        {
            LOG_TEST(log, "Not found part {} on replica {}", part->description.info.getPartNameV1(), replica_num);
            continue;
        }

        response.description.push_back({
            .info = part->description.info,
            .ranges = {},
        });

        while (!part->description.ranges.empty() && current_mark_size < min_number_of_marks)
        {
            auto & range = part->description.ranges.front();
            const size_t needed = min_number_of_marks - current_mark_size;

            if (range.getNumberOfMarks() > needed)
            {
                auto range_we_take = MarkRange{range.begin, range.begin + needed};
                response.description.back().ranges.emplace_back(range_we_take);
                current_mark_size += range_we_take.getNumberOfMarks();

                range.begin += needed;
                break;
            }

            response.description.back().ranges.emplace_back(range);
            current_mark_size += range.getNumberOfMarks();
            part->description.ranges.pop_front();
        }
    }
}

ParallelReadResponse DefaultCoordinator::handleRequest(ParallelReadRequest request)
{
    LOG_TRACE(log, "Handling request from replica {}, minimal marks size is {}", request.replica_num, request.min_number_of_marks);

    size_t current_mark_size = 0;
    ParallelReadResponse response;

    /// 1. Try to select from preferred set of parts for current replica
    selectPartsAndRanges(reading_state[request.replica_num], request.replica_num, request.min_number_of_marks, current_mark_size, response);

    /// 2. Try to use parts from delayed queue
    while (!delayed_parts.empty() && current_mark_size < request.min_number_of_marks)
    {
        auto part = delayed_parts.front();
        delayed_parts.pop_front();
        reading_state[request.replica_num].emplace_back(part);
        selectPartsAndRanges(reading_state[request.replica_num], request.replica_num, request.min_number_of_marks, current_mark_size, response);
    }

    /// 3. Try to steal tasks;
    if (current_mark_size < request.min_number_of_marks)
    {
        for (size_t i = 0; i < replicas_count; ++i)
        {
            if (i != request.replica_num)
                selectPartsAndRanges(reading_state[i], request.replica_num, request.min_number_of_marks, current_mark_size, response);

            if (current_mark_size >= request.min_number_of_marks)
                break;
        }
    }

    stats[request.replica_num].number_of_requests += 1;
    stats[request.replica_num].sum_marks += current_mark_size;

    if (response.description.empty())
        response.finish = true;

    LOG_TRACE(log, "Going to respond to replica {} with {}", request.replica_num, response.describe());
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
    void handleInitialAllRangesAnnouncement([[ maybe_unused ]]  InitialAllRangesAnnouncement announcement) override;
    void markReplicaAsUnavailable(size_t replica_number) override;

    Parts all_parts_to_read;
    size_t total_rows_to_read = 0;

    Poco::Logger * log = &Poco::Logger::get(fmt::format("{}{}", magic_enum::enum_name(mode), "Coordinator"));
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
void InOrderCoordinator<mode>::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    LOG_TRACE(log, "Received an announcement {}", announcement.describe());

    size_t new_rows_to_read = 0;

    /// To get rid of duplicates
    for (auto && part: announcement.description)
    {
        auto the_same_it = std::find_if(all_parts_to_read.begin(), all_parts_to_read.end(),
            [&part] (const Part & other) { return other.description.info == part.info; });

        /// We have the same part - add the info about presence on current replica to it
        if (the_same_it != all_parts_to_read.end())
        {
            the_same_it->replicas.insert(announcement.replica_num);
            continue;
        }

        auto covering_or_the_same_it = std::find_if(all_parts_to_read.begin(), all_parts_to_read.end(),
            [&part] (const Part & other) { return other.description.info.contains(part.info) ||  part.info.contains(other.description.info); });

        /// It is covering part or we have covering - skip it
        if (covering_or_the_same_it != all_parts_to_read.end())
            continue;

        new_rows_to_read += part.rows;

        auto [inserted_it, _] = all_parts_to_read.emplace(Part{.description = std::move(part), .replicas = {announcement.replica_num}});
        auto & ranges = inserted_it->description.ranges;
        std::sort(ranges.begin(), ranges.end());
    }

    if (new_rows_to_read > 0)
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

    LOG_TRACE(log, "Got request from replica {}, data {}", request.replica_num, request.describe());

    ParallelReadResponse response;
    response.description = request.description;
    size_t overall_number_of_marks = 0;

    for (auto & part : response.description)
    {
        auto global_part_it = std::find_if(all_parts_to_read.begin(), all_parts_to_read.end(),
            [&part] (const Part & other) { return other.description.info == part.info; });

        if (global_part_it == all_parts_to_read.end())
            continue;

        if (!global_part_it->replicas.contains(request.replica_num))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} doesn't exist on replica {} according to the global state", part.info.getPartNameV1(), request.replica_num);

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
    std::lock_guard lock(mutex);

    if (!pimpl)
    {
        mode = announcement.mode;
        initialize();
    }

    return pimpl->handleInitialAllRangesAnnouncement(std::move(announcement));
}

ParallelReadResponse ParallelReplicasReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    std::lock_guard lock(mutex);

    if (!pimpl)
    {
        mode = request.mode;
        initialize();
    }

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
    std::lock_guard lock(mutex);

    if (!pimpl)
        unavailable_nodes_registered_before_initialization.push_back(replica_number);
    else
        pimpl->markReplicaAsUnavailable(replica_number);
}

void ParallelReplicasReadingCoordinator::initialize()
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

    if (progress_callback)
        pimpl->setProgressCallback(std::move(progress_callback));

    for (const auto replica : unavailable_nodes_registered_before_initialization)
        pimpl->markReplicaAsUnavailable(replica);
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator(size_t replicas_count_) : replicas_count(replicas_count_) {}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

void ParallelReplicasReadingCoordinator::setProgressCallback(ProgressCallback callback)
{
    // store callback since pimpl can be not instantiated yet
    progress_callback = std::move(callback);
    if (pimpl)
        pimpl->setProgressCallback(std::move(progress_callback));
}

}
