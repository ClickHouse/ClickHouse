#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Core/Block.h>

#include <atomic>

/// What the runs of a query of a multi-pass top-K aggregation keep for its next runs.

namespace DB::MultiPassTopKImpl
{

/// After a pilot declines, the next runs of the same query skip the pilot and run the original aggregation,
/// until this many have, when a pilot checks the decision again against the data of then.
constexpr Int32 runs_without_pilot_after_decline = 15;

/// What a run of a query found that its next runs start from instead of a pilot: the mode it ran, and for the
/// candidate mode the keys of the candidates, which only the setting `adaptive_aggregator_top_k_cache_candidates`
/// keeps. A memo holds no aggregate and no bound: every run proves its own result, so a memo that the data outdated
/// costs time, not correctness.
struct TopKMemo
{
    enum class Mode : uint8_t
    {
        /// The pilot declined, the candidates needed the fallback where they may not learn from it (see `learned`), or
        /// the bounding mode aggregated most rows: the next runs run the original aggregation.
        Original,
        /// The candidate pass with `candidate_keys` proved the top, or a fallback completed them: the next runs start
        /// with it.
        Candidates,
        /// The bounding mode ran, and its restricted passes aggregated few rows: the next runs start with its sketch.
        Bounding,
    };

    TopKMemo(Mode mode_, Block candidate_keys_, bool learned_)
        : mode(mode_)
        , candidate_keys(std::move(candidate_keys_))
        , learned(learned_)
    {
    }

    const Mode mode;
    const Block candidate_keys;
    /// The candidates hold the keys that a fallback found heavy, which the sample missed. When they need the fallback
    /// again, its cells hold many keys of medium ranks, which no candidates prove, so the next runs run the original
    /// aggregation.
    const bool learned;
    /// For `Mode::Original`: the runs left that skip the pilot. Only a decline expires: the original aggregation shows
    /// nothing about whether a pilot would decide otherwise now, while the other modes check themselves on every run,
    /// by the proof of the candidates or by the share of the rows that the restricted passes aggregate.
    std::atomic<Int32> runs_without_pilot = runs_without_pilot_after_decline;
};

using TopKMemoPtr = std::shared_ptr<TopKMemo>;

/// The memo that a new run of the query of `templ` starts from, if one applies. An `Original` memo applies to
/// `runs_without_pilot_after_decline` runs, after which a pilot checks the decision again, so the lookup counts the new
/// run against it: call it once per run. The key is a hash, which another query may share, so the candidates must have
/// the types of the keys, and the bounding mode needs a bounding key.
TopKMemoPtr memoForRun(const MultiPassTopKTemplate & templ);

/// What a run of a query starts from, and what it keeps for the next runs (see `TopKMemo`).
class TopKRunMemo
{
public:
    /// `start` is the memo of an earlier run that the run starts from instead of a pilot (see `memoForRun`), if any.
    TopKRunMemo(const MultiPassTopKTemplate & templ, TopKMemoPtr start_);

    bool startedFromMemo() const { return start != nullptr; }
    /// The run started with candidates that a fallback completed (see `TopKMemo::learned`).
    bool startedFromLearned() const { return start && start->learned; }
    /// The next runs may start from the candidates of the run: the hash table statistics identify them, and the setting
    /// `adaptive_aggregator_top_k_cache_candidates` allows it.
    bool cachesCandidates() const { return cache_key && caches_candidates; }

    /// The next runs of the query run the original aggregation, start with the candidate pass with `keys`, or start
    /// with the sketch pass.
    void keepOriginal() { keep(TopKMemo::Mode::Original, {}, /*learned=*/false); }
    void keepCandidates(Block keys, bool learned) { keep(TopKMemo::Mode::Candidates, std::move(keys), learned); }
    void keepBounding() { keep(TopKMemo::Mode::Bounding, {}, /*learned=*/false); }

private:
    void keep(TopKMemo::Mode mode, Block keys, bool learned);

    /// Zero when the hash table statistics are disabled: the run keeps nothing then.
    const UInt64 cache_key;
    const bool caches_candidates;
    const TopKMemoPtr start;
};

using TopKRunMemoPtr = std::shared_ptr<TopKRunMemo>;

}
