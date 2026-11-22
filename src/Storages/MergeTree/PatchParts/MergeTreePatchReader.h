#pragma once
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PatchParts/applyPatches.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Core/Block.h>
#include <Common/CacheBase.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

class MergeTreePatchReader : private boost::noncopyable
{
public:
    using ReadResult = MergeTreeRangeReader::ReadResult;

    MergeTreePatchReader(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);
    virtual ~MergeTreePatchReader() = default;

    virtual std::vector<PatchReadResultPtr> readPatches(MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & main_block,
        const PatchReadResult * last_read_patch) = 0;

    /// Returns true if we need to keep old_patch for main_result.
    /// An old patch is not needed if main_result and all further results have data newer than covered by old_patch.
    virtual bool needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & main_block) const = 0;

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

    std::vector<PatchReadResultPtr> readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & main_block,
        const PatchReadResult * last_read_patch) override;

    bool needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & main_block) const override;

private:
    PatchReadResultPtr readPatch(const MarkRange & range);
    /// Returns true if we need to read a new patch part for main_result.
    /// A new patch is needed if main_result has newer data than covered by old_patch.
    bool needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const;
};

class MergeTreePatchReaderJoin : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchJoinCache * patch_join_cache_);

    std::vector<PatchReadResultPtr> readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & main_block,
        const PatchReadResult * last_read_patch) override;

    /// Return true because patch with Join mode is shared between all data
    /// in range and we shouldn't remove it until reading of range is finished.
    bool needOldPatch(const ReadResult &, const PatchReadResult &, const Block &) const override { return true; }

private:
    PatchJoinCache * patch_join_cache;
};

/// V2 reader. Streams the patch part mark-range by mark-range in sort-key order.
class MergeTreePatchReaderMergeOnKey : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderMergeOnKey(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);

    std::vector<PatchReadResultPtr> readPatches(
        MarkRanges & ranges,
        const ReadResult & main_result,
        const Block & main_block,
        const PatchReadResult * last_read_patch) override;

    /// Compares the main result's min sort-key tuple against the cached patch block's
    /// max sort-key tuple, so patch blocks are evicted as the main cursor advances past their range.
    bool needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & main_block) const override;

private:
    PatchReadResultPtr readPatch(const MarkRange & range);
    bool needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & main_block) const;
};

using MergeTreePatchReaderPtr = std::shared_ptr<MergeTreePatchReader>;
using MergeTreePatchReaders = std::vector<MergeTreePatchReaderPtr>;

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchJoinCache * read_join_cache);

}
