#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>

namespace DB
{

struct ProjectionDescription;

struct MergeTreeIndexGranuleProjection final : public MergeTreeIndexGranuleText
{
    explicit MergeTreeIndexGranuleProjection(const String & projection_name_);
    ~MergeTreeIndexGranuleProjection() override;

    void deserializeBinaryWithMultipleStreams(MergeTreeIndexInputStreams & streams, MergeTreeIndexDeserializationState & state) override;

    static PostingListPtr
    materializeFromTokenInfo(LargePostingListReaderStream & stream, const TokenPostingsInfo & token_info, size_t block_idx);

    String projection_name;

    /// TODO(amos): Do we need per-token stream to reduce seek?
    LargePostingListReaderStreamPtr large_posting_stream;
};

class MergeTreeIndexProjection final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexProjection(const ProjectionDescription & projection, std::shared_ptr<const MergeTreeIndexText> text_index_);

    ~MergeTreeIndexProjection() override = default;

    bool supportsReadingOnParallelReplicas() const override { return true; }
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
