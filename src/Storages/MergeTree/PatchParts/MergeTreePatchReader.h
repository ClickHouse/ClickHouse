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

    struct PatchReadResult
    {
        PatchReadResult(ReadResult read_result_, PatchSharedDataPtr data_)
            : read_result(std::move(read_result_)), data(std::move(data_))
        {
        }

        ReadResult read_result;
        PatchSharedDataPtr data;
    };

    using PatchReadResultPtr = std::shared_ptr<const PatchReadResult>;

    MergeTreePatchReader(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);
    virtual ~MergeTreePatchReader() = default;

    virtual PatchReadResultPtr readPatch(MarkRanges & ranges) = 0;
    virtual PatchToApplyPtr applyPatch(const Block & result_block, const PatchReadResult & patch_result) const = 0;

    /// Returns true if we need to read a new patch part for main_result.
    /// A new patch is needed if main_result has newer data than covered by old_patch.
    virtual bool needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const = 0;

    /// Returns true if we need to keep old_patch for main_result.
    /// An old patch is not needed if main_result and all further results have data newer than covered by old_patch.
    virtual bool needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const = 0;

    const PatchPartInfoForReader & getPatchPart() const { return patch_part; }
    Block getHeader() const { return range_reader.getSampleBlock(); }
    IMergeTreeReader * getReader() const { return reader.get(); }

protected:
    ReadResult readPatchRange(MarkRanges ranges);

    PatchPartInfoForReader patch_part;
    MergeTreeReaderPtr reader;
    MergeTreeRangeReader range_reader;
};

class PatchReadResultCache : public CacheBase<UInt128, MergeTreePatchReader::PatchReadResult>
{
public:
    PatchReadResultCache();
    static UInt128 hash(const String & patch_name, const MarkRanges & ranges);
};

using PatchReadResultCachePtr = std::shared_ptr<PatchReadResultCache>;

class MergeTreePatchReaderMerge : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderMerge(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_);

    PatchReadResultPtr readPatch(MarkRanges & ranges) override;
    PatchToApplyPtr applyPatch(const Block & result_block, const PatchReadResult & patch_result) const override;
    bool needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const override;
    bool needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const override;
};

class MergeTreePatchReaderJoin : public MergeTreePatchReader
{
public:
    MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchReadResultCache * read_result_cache_);

    PatchReadResultPtr readPatch(MarkRanges & ranges) override;
    PatchToApplyPtr applyPatch(const Block & result_block, const PatchReadResult & patch_result) const override;
    /// Return true because we need to read all data in range for Join mode.
    bool needNewPatch(const ReadResult &, const PatchReadResult &) const override { return true; }
    /// Return true because patch with Join mode is shared between all data
    /// in range and we shouldn't remove it until reading of range is finished.
    bool needOldPatch(const ReadResult &, const PatchReadResult &) const override { return true; }

private:
    PatchReadResultCache * read_result_cache;
};

using MergeTreePatchReaderPtr = std::shared_ptr<MergeTreePatchReader>;
using MergeTreePatchReaders = std::vector<MergeTreePatchReaderPtr>;

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchReadResultCache * read_result_cache);

}
