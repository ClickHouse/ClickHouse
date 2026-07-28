#include <Storages/MergeTree/UniqueKey/UniqueKeyProbeSimple.h>

#include <Storages/MergeTree/UniqueKey/UniqueKeyEncoding.h>

#include <Core/Block.h>

#include <Common/Exception.h>

#include <string_view>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Debug builds validate the "a live key lives in <= 1 part" invariant by probing
/// every part for every key; release builds early-skip a key once it resolves live.
#ifndef NDEBUG
static constexpr bool probe_validates_single_live_part = true;
#else
static constexpr bool probe_validates_single_live_part = false;
#endif

UniqueKeyProbeSimple::UniqueKeyProbeSimple(
    ProbeTargetsSupplier supplier_,
    Names unique_key_column_names_,
    size_t max_encoded_size_)
    : supplier(std::move(supplier_))
    , unique_key_column_names(std::move(unique_key_column_names_))
    , max_encoded_size(max_encoded_size_)
{
}

std::vector<ProbeResult> UniqueKeyProbeSimple::probeBatch(const Block & keys, const String & partition_id)
{
    const size_t n = keys.rows();
    std::vector<ProbeResult> results(n);
    if (n == 0)
        return results;

    /// Encode the whole key batch once, in unique-key column order — targets
    /// consume the encoded bytes and never re-encode.
    Columns uk_columns;
    uk_columns.reserve(unique_key_column_names.size());
    for (const auto & name : unique_key_column_names)
        uk_columns.push_back(keys.getByName(name).column);

    std::vector<String> encoded;
    UniqueKeyEncoding::encodeBlock(uk_columns, /*permutation=*/nullptr, max_encoded_size, encoded);

    std::vector<std::string_view> views;
    views.reserve(n);
    for (const auto & e : encoded)
        views.emplace_back(e.data(), e.size());

    /// Snapshot captured once — the caller holds the active-parts list stable
    /// for the batch. Walk newest-first: the first live hit per key wins;
    /// otherwise track whether the key was seen only in bitmap-dead rows.
    auto snapshot = supplier(partition_id);

    std::vector<char> resolved(n, 0); /// found live → no later part can override
    std::vector<char> any_dead(n, 0);
    std::vector<std::optional<UInt64>> batch_out;

    for (const auto & target : snapshot)
    {
        if (!target)
            continue;

        target->findRowIndexBatch(views, batch_out);
        const IMergeTreeDataPart * underlying = target->getUnderlyingPart();

        for (size_t i = 0; i < n; ++i)
        {
            /// See `probe_validates_single_live_part`: release skips, debug does not.
            if (resolved[i] && !probe_validates_single_live_part)
                continue;

            const auto & row_opt = batch_out[i];
            if (!row_opt.has_value())
                continue;

            if (!target->isRowDead(*row_opt))
            {
                if (resolved[i])
                {
                    /// Debug-only: a second live hit violates the single-live-part invariant.
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "UNIQUE KEY probe invariant violated: key at batch row {} is live in more "
                        "than one part (partition '{}')", i, partition_id);
                }

                results[i].outcome = ProbeOutcome::FOUND_LIVE;
                results[i].part = underlying;
                results[i].row_number = *row_opt;
                resolved[i] = 1;
            }
            else
            {
                any_dead[i] = 1;
            }
        }
    }

    for (size_t i = 0; i < n; ++i)
        if (!resolved[i] && any_dead[i])
            results[i].outcome = ProbeOutcome::FOUND_ALL_DEAD;

    return results;
}

}
