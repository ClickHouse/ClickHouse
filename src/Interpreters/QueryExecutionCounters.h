#pragma once

#include <Core/Joins.h>
#include <base/defines.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

namespace DB
{

class IJoin;

/// Metrics about the execution of a query, dumped into `system.query_log`.
/// All of them are best effort and record nothing when the thread is not attached to a query.
struct QueryExecutionCounters
{
    /// Records one physical join, taking its kind, strictness and algorithm from the join itself.
    static void addExecutedJoin(const IJoin & join);

    /// For a join whose algorithm the `IJoin` alone cannot name, because it is decided while
    /// the pipeline is assembled.
    static void addExecutedJoin(const IJoin & join, std::string_view algorithm);

    /// For a join that has no `IJoin` at all because the whole algorithm lives in a query plan step,
    /// like `ie_join`. `kind` and `strictness` must be the ones the query asked for.
    static void addExecutedJoin(JoinKind kind, JoinStrictness strictness, std::string_view algorithm);

    /// Names a pipeline that is not assembled here, but will be assembled - and assembled again - while
    /// the query runs: the relation of a `loop` and the recursive member of a recursive CTE are built by
    /// their source, long after the pipeline that holds them was put together.
    ///
    /// `kind` only makes the name readable, it does not have to be unique.
    static String makeScopeForPipelineBuiltLater(std::string_view kind);

    static void addUsedJoinAlgorithm(JoinAlgorithm algorithm);

    static void markSpilledToDisk(std::string_view operator_name);

    /// A region in which the pipeline of one query is built more than once, e.g. a materialized view.
    class [[nodiscard]] RepeatedPipelineBuildScope : private boost::noncopyable
    {
    public:
        explicit RepeatedPipelineBuildScope(String scope);
        ~RepeatedPipelineBuildScope();

    private:
        String scope_to_restore;
        size_t registered_joins_to_restore;
        size_t named_pipelines_to_restore;
    };

    /// A consistent copy of all the counters.
    struct Snapshot
    {
        UInt64 number_of_joins = 0;
        std::set<String> join_algorithms;
        std::vector<String> join_kinds;
        std::vector<String> join_strictness;
        std::set<String> spilled_to_disk;
    };

    /// Takes all the counters under a single lock, so that the result cannot mix values recorded before
    /// and after a late update.
    Snapshot getSnapshot() const;

private:

    mutable std::mutex mutex;

    /// Counters of the query the calling thread belongs to, or nullptr when it is not attached to one.
    static std::shared_ptr<QueryExecutionCounters> getForCurrentQuery();

    /// Algorithms that were used in the query
    std::set<String> used_join_algorithms TSA_GUARDED_BY(mutex);

    /// One element per physical join executed by the query, which is also how `number_of_joins` is counted.
    /// Keeps both elements together, to avoid mis-aligned items.
    std::multiset<std::pair<String, String>> used_joins TSA_GUARDED_BY(mutex);

    /// Operators that wrote temporary data to disk
    std::set<String> spilled_to_disk TSA_GUARDED_BY(mutex);

    /// The joins that were registered inside a `RepeatedPipelineBuildScope`, by the scope and the ordinal
    /// of the join inside the build, so that another build of the same pipeline does not count them once
    /// more.
    std::set<std::pair<String, size_t>> joins_of_repeated_builds TSA_GUARDED_BY(mutex);

    /// Registers a join in `joins_of_repeated_builds` and tells whether an earlier build of the same
    /// pipeline already counted it. Outside a `RepeatedPipelineBuildScope` there is nothing to
    /// deduplicate and the answer is always false.
    bool isCountedByAnEarlierBuild() TSA_REQUIRES(mutex);
};

using QueryExecutionCountersPtr = std::shared_ptr<QueryExecutionCounters>;

}
