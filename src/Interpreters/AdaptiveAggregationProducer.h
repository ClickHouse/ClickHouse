#pragma once

#include <memory>
#include <variant>

#include <Interpreters/AdaptiveAggregationSession.h>
#include <Interpreters/AdaptiveAggregationStaging.h>

namespace DB
{

class Aggregator;

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase and its
/// phase-owned counters, and the thread's staged-chunk builder (created by
/// `Aggregator::createAdaptiveProducer`, which knows the aggregate-argument positions the
/// builder gathers).
struct AdaptiveAggregationProducer
{
    AdaptiveAggregationProducer(AdaptiveAggregationSessionPtr shared_, StagedChunkBuilder staging_)
        : session(std::move(shared_))
        , staging(std::move(staging_))
    {
    }

    /// The thread starts learning: the local table inserts as usual while this rule watches
    /// its growth between blocks and decides the phase transition.
    struct LearningState
    {
        enum class Verdict
        {
            KeepLearning,
            /// The table reached the freeze threshold: switch to the frozen kernel.
            Freeze,
            /// A table that consumed many times the threshold in rows while staying below it
            /// in keys is repeat-dominated and will not freeze in practice: either the group
            /// count plateaus below the threshold (few groups with fat states, e.g.
            /// `uniqExact` per region, where the freeze would foreclose the byte-triggered
            /// conversion and its bucket-parallel merge), or the hot share is so extreme that
            /// staging the sliver of a tail cannot pay. The thread falls back to the baseline
            /// path, permanently.
            GiveUp,
        };

        Verdict decide(size_t added_rows, size_t keys, size_t freeze_threshold, bool convertible_to_two_level)
        {
            if (keys >= freeze_threshold && convertible_to_two_level)
                return Verdict::Freeze;
            rows_seen += added_rows;
            if (rows_seen >= give_up_row_multiple * freeze_threshold && keys < freeze_threshold)
                return Verdict::GiveUp;
            return Verdict::KeepLearning;
        }

        /// The give-up bound balances two constraints: it caps the tolerated hot share at
        /// 1 - 1/multiple (a 90% hot key still freezes with a wide margin), and it must fire
        /// within the rows one thread sees on a medium table at a wide fan-out (a 64-thread
        /// scan of 50M rows gives each thread less than a million rows).
        static constexpr size_t give_up_row_multiple = 16;

        size_t rows_seen = 0;
    };

    /// The adaptive phase proper: the local table only updates the keys it already holds
    /// and misses are staged for the shared drain. Carries the post-freeze hit-rate
    /// sampling: when the frozen table turns out to hold almost none of the stream's keys
    /// (a uniform high-cardinality distribution), probing it is pure overhead on every row;
    /// after the sample window the kernel switches to staging every row without the lookup.
    struct FrozenState
    {
        /// Feeds one block's probe outcome in; returns true when this call decided the
        /// bypass, so the caller can record the event once.
        bool recordProbeSample(size_t hits, size_t rows)
        {
            if (bypass_local_probe)
                return false;
            sampled_hits += hits;
            sampled_rows += rows;
            if (sampled_rows >= sample_rows && sampled_hits * hit_rate_inverse < sampled_rows)
            {
                bypass_local_probe = true;
                return true;
            }
            return false;
        }

        /// The sample window must be reachable by every stream: at 64 threads a thread sees
        /// 1/64th of the input, so a filtered ~13M-row aggregation still leaves ~200K rows
        /// per thread. Probing the frozen table pays off only while at least one row in
        /// `hit_rate_inverse` hits it.
        static constexpr size_t sample_rows = 65'536;
        static constexpr size_t hit_rate_inverse = 4;

        size_t sampled_rows = 0;
        size_t sampled_hits = 0;
        bool bypass_local_probe = false;
    };

    /// Terminal: the thread aggregates exactly as with the feature off, keeping only the
    /// reason it stood down.
    struct BaselineState
    {
        enum class Reason
        {
            /// The give-up rule: the table stayed far below the freeze threshold across
            /// many times that many rows, so the stream is repeat-dominated locally.
            TooFewDistinctKeys,
            /// The global thaw: the session-wide staged-key sample proved the whole stream
            /// repeat-dominated (see `stageRecordedMisses`).
            RepeatedStagedKeys,
        };
        Reason reason;
    };

    using Phase = std::variant<LearningState, FrozenState, BaselineState>;
    Phase phase = LearningState{};

    bool isLearning() const { return std::holds_alternative<LearningState>(phase); }
    bool isFrozen() const { return std::holds_alternative<FrozenState>(phase); }
    bool isBaseline() const { return std::holds_alternative<BaselineState>(phase); }

    void freeze() { phase = FrozenState{}; }
    void standDown(BaselineState::Reason reason) { phase = BaselineState{.reason = reason}; }

    AdaptiveAggregationSessionPtr session;

    StagedChunkBuilder staging;

    /// Where this producer's sealed staged chunks go, chosen and installed by the transform
    /// that owns the producer (see `AggregatingTransform`) right after construction, and
    /// fixed for the producer's lifetime.
    std::unique_ptr<IStagedChunkSink> staging_sink;
};

/// The production-time destination of sealed staged chunks: finishes the chunk and publishes
/// it to the session's per-bucket backlogs (see `Aggregator::publishStagedChunk`).
struct StagedChunkBacklogSink final : IStagedChunkSink
{
    StagedChunkBacklogSink(const Aggregator & aggregator_, AdaptiveAggregationSession & session_)
        : aggregator(aggregator_), session(session_)
    {
    }

    void consume(MutableStagedChunkPtr chunk) override;

private:
    const Aggregator & aggregator;
    AdaptiveAggregationSession & session;
};

}
