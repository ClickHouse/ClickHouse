#pragma once

#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>

namespace DB
{

class MergeTreeReaderProjectionIndex : public MergeTreeReaderTextIndex
{
public:
    MergeTreeReaderProjectionIndex(
        const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_, NamesAndTypesList columns_, bool can_skip_mark_);

    void prefetchBeginOfRange(Priority /* priority */) override { }

private:
    PostingListPtr readPostingsBlockForToken(std::string_view token, const TokenPostingsInfo & token_info, size_t block_idx) override;
};

}
