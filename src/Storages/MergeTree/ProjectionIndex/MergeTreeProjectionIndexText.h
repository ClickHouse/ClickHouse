#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionTokenInfo.h>

namespace DB
{

struct ProjectionDescription;

/// Projection text index granule — independent from skip-index MergeTreeIndexGranuleText.
///
/// Inherits IMergeTreeIndexGranule directly (not MergeTreeIndexGranuleText which is final)
/// and owns all its state. This decouples the projection text index from upstream
/// skip-index implementation changes, at the cost of ~100 lines of self-contained
/// field declarations and method implementations.
struct MergeTreeProjectionIndexGranuleText final : public IMergeTreeIndexGranule
{
    explicit MergeTreeProjectionIndexGranuleText(const String & projection_name_);
    ~MergeTreeProjectionIndexGranuleText() override;

    void serializeBinary(WriteBuffer &) const override;
    void deserializeBinary(ReadBuffer &, MergeTreeIndexVersion) override;
    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    bool empty() const override { return is_empty; }
    size_t memoryUsageBytes() const override { return 0; /* TODO if needed */ }
    void setCurrentRange(RowsRange range) override { current_range = std::move(range); }

    /// Mark filtering: precise packed-block decoding via DecodedBlockCache.
    bool hasAnyQueryTokens(const TextSearchQuery & query) const;
    bool hasAnyQueryPatterns(const TextSearchQuery & query) const;
    bool hasAllQueryTokens(const TextSearchQuery & query) const;
    bool hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const;

    static PostingListPtr
    materializeFromTokenInfo(LargePostingListReaderStream & stream, const ProjectionTokenInfo & token_info, size_t block_idx);

    /// Accessors for MergeTreeReaderTextProjectionIndex
    const ProjectionTokenInfoMap & getRemainingTokens() const { return remaining_tokens; }
    PostingListPtr getPostingsForRareToken(std::string_view token) const;

    // --- Fields ---

    String projection_name;
    bool is_empty = true;
    bool has_block_index = true;

    /// Projection part reference (set during deserialization)
    MergeTreeDataPartPtr projection_part;

    /// Stream for reading large posting lists from projection part
    LargePostingListReaderStreamPtr large_posting_stream;

    /// Token info populated during deserialization — uses ProjectionTokenInfo (not upstream TokenPostingsInfo)
    ProjectionTokenInfoMap remaining_tokens;
    TokenToPostingsMap rare_tokens_postings;

    /// Current range for precise mark filtering
    std::optional<RowsRange> current_range;

    /// Cache key prefix for TextIndexTokensCache / TextIndexPostingsCache
    String index_id_for_caches;

    /// Query-scoped cache of decoded packed blocks, populated during mark
    /// filtering and consumed by ProjectionPostingListCursor during data read.
    mutable std::unique_ptr<DecodedBlockCache> decoded_block_cache;

    /// .pst stream for decoding packed blocks during mark filtering.
    mutable LargePostingListReaderStreamPtr pst_stream;

private:
    std::vector<String> fillTokensFromCache(MergeTreeIndexDeserializationState & state);
};

class MergeTreeProjectionIndexText final : public IMergeTreeIndex
{
public:
    explicit MergeTreeProjectionIndexText(const ProjectionDescription & projection, std::shared_ptr<const MergeTreeIndexText> text_index_);

    MergeTreeProjectionIndexText(const IndexDescription & index_description, std::shared_ptr<const MergeTreeIndexText> text_index_);

    ~MergeTreeProjectionIndexText() override = default;

    bool isTextIndex() const override { return true; }
    bool isProjectionIndex() const override { return true; }

    MergeTreeIndexSubstreams getSubstreams() const override;
    MergeTreeIndexFormat
    getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & path_prefix) const override;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    std::shared_ptr<const MergeTreeIndexText> text_index;
};

}
