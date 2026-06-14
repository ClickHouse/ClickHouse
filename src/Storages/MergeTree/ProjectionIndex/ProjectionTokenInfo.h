#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

/// Projection-specific token posting info.
///
/// Inherits from `TokenInfoBase` (the common base shared with skip-index's
/// `TokenPostingsInfo`) to participate in the shared `TextIndexTokensCache`.
///
/// Contains only the fields the projection text index actually needs:
///   - `cardinality` + `ranges` — inherited from `TokenInfoBase`
///   - `large_block_metas` — full per-block metadata (LargePostingBlockMeta)
///   - `first_doc_freq` — frequency of the first doc_id (for phrase queries)
///   - `first_doc_pos_offset` — position data offset for the first doc_id
///   - `block_index_data` — pre-materialized .pidx data (packed block ranges,
///     cumulative bytes, position skip data). This is the key advantage over
///     the upstream cache: the skip-index path does not cache the decoded
///     block index, whereas the projection caches it so that subsequent
///     queries reuse already-decoded `LargeBlockData` for both mark filtering
///     and `PostingListCursor` data reads.
///
/// Does NOT inherit from `TokenPostingsInfo` — avoids carrying unused fields
/// (`header`, `offsets` as UInt64, `embedded_postings`).
///
/// Cache retrieval uses `dynamic_pointer_cast<ProjectionTokenInfo>` on the
/// `TokenInfoBase` returned by `TextIndexTokensCache`. A failed cast (e.g.
/// if a skip-index entry with the same key were retrieved) is treated as a
/// cache miss — safe by construction. In practice, cache keys are namespaced
/// by `index_id_for_caches` (skip-index uses `skp_idx_{name}`, projection
/// uses `proj:{name}`), so cross-type collisions do not occur.
struct ProjectionTokenInfo : public TokenInfoBase
{
    absl::InlinedVector<LargePostingBlockMeta, 1> large_block_metas;

    UInt32 first_doc_freq = 0;
    UInt64 first_doc_pos_offset = 0;
    std::vector<std::shared_ptr<LargeBlockData>> block_index_data;

    size_t bytesAllocated() const override;

    /// Build a ProjectionTokenInfo from a deserialized PostingListStream.
    /// Extracts cardinality, ranges, large_block_metas from the stream,
    /// and decodes .pidx block index data if pidx_stream is provided.
    /// Returns nullptr if the token has zero cardinality.
    static std::shared_ptr<ProjectionTokenInfo> buildFromPostingStream(
        const PostingListStream & stream,
        LargePostingListReaderStream * pidx_stream,
        bool phrase_enabled);

    /// Check whether this token's posting list has any doc in [range_begin, range_end].
    /// Encapsulates binary search on packed_block_ranges + precise TurboPFor decode.
    bool hasDocInRange(
        const RowsRange & current_range,
        UInt32 range_begin,
        UInt32 range_end,
        std::unique_ptr<DecodedBlockCache> & cache,
        LargePostingListReaderStream * pst_stream) const;
};

using ProjectionTokenInfoPtr = std::shared_ptr<ProjectionTokenInfo>;
using ProjectionTokenInfoMap = absl::flat_hash_map<String, ProjectionTokenInfoPtr>;

}
