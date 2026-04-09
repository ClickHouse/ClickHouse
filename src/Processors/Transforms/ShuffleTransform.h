#pragma once

#include <optional>
#include <Processors/Port.h>
#include <Processors/IAccumulatingTransform.h>
#include <pcg_random.hpp>

namespace DB
{

/** Randomly permutes all input rows.
  *
  * Without a limit: accumulates the entire input, applies a global
  * Fisher-Yates shuffle, and emits every row in uniformly random order.
  * Time O(n), space O(n).
  *
  * With a limit k: applies Algorithm L reservoir sampling. Fills an
  * O(k) reservoir in a single pass and emits exactly min(n, k) rows
  * in uniformly random order. Time O(n), space O(k).
  */
class ShuffleTransform : public IAccumulatingTransform
{
public:
    explicit ShuffleTransform(SharedHeader header, std::optional<size_t> limit_ = std::nullopt);

    String getName() const override { return "ShuffleTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    std::optional<size_t> limit;
    pcg64 rng;
    bool generated = false;

    // ---- Fisher-Yates mode (no limit) ----
    Chunks accumulated;

    // ---- Algorithm L reservoir mode (with limit) ----
    struct ReservoirSlot
    {
        MutableColumns columns; ///< One column per output column, each holding exactly one row.
    };
    std::vector<ReservoirSlot> reservoir_slots;
    size_t rows_seen = 0;   ///< Total rows processed so far.
    double w = 0.0;         ///< Algorithm L weight factor.
    size_t next_replace = 0; ///< Absolute row index at which the next replacement occurs.

    void initAlgorithmL();
    void advanceAlgorithmL(size_t current_index);
    void consumeReservoir(Chunk chunk);
    Chunk generateReservoir();
    Chunk generateFullShuffle();
};

}
