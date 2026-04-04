#pragma once
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Core/Block.h>
#include <Common/CacheBase.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

struct PatchJoinCache;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

class MergeTreePatchReader : private boost::noncopyable
{
public:
    using ReadResult = MergeTreeRangeReader::ReadResult;

    MergeTreePatchReader(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);
    virtual ~MergeTreePatchReader() = default;

    virtual PatchReadResultPtr createResult() const = 0;

    virtual void readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & result_header,
        PatchReadResult & result) = 0;

    virtual std::vector<PatchToApplyPtr> applyPatch(const Block & result_block, const PatchReadResult & patch_result) const = 0;

    const PatchPartInfoForReader & getPatchPart() const { return patch_part; }
    Block getHeader() const { return range_reader.getSampleBlock(); }
    IMergeTreeReader * getReader() const { return reader.get(); }

protected:
    ReadResult readPatchRanges(MarkRanges ranges);

    PatchPartInfoForReader patch_part;
    MergeTreeReaderPtr reader;
    MergeTreeRangeReader range_reader;
};

class MergeTreePatchReaderMerge : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderMerge(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);

    PatchReadResultPtr createResult() const override;

    void readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & result_header,
        PatchReadResult & result) override;

    std::vector<PatchToApplyPtr> applyPatch(const Block & result_block, const PatchReadResult & patch_result) const override;

private:
    struct ReadBlockInfo
    {
        ConstBlockPtr block;
        UInt64 min_part_offset = 0;
        UInt64 max_part_offset = 0;
    };

    ReadBlockInfo readPatch(const MarkRange & range);
    /// Returns true if we need to read a new patch part for main_result.
    /// A new patch is needed if main_result has newer data than covered by the last read patch.
    bool needNewPatch(const ReadResult & main_result, const PatchMergeReadResult & result) const;
};

class MergeTreePatchReaderJoin : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchJoinCache * patch_join_cache_);

    PatchReadResultPtr createResult() const override;

    void readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & result_header,
        PatchReadResult & result) override;

    std::vector<PatchToApplyPtr> applyPatch(const Block & result_block, const PatchReadResult & patch_result) const override;

private:
    PatchJoinCache * patch_join_cache;
};

using MergeTreePatchReaderPtr = std::shared_ptr<MergeTreePatchReader>;
using MergeTreePatchReaders = std::vector<MergeTreePatchReaderPtr>;

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchJoinCache * read_join_cache);

}
