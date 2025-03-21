#pragma once
#include <Storages/MergeTree/MergeTreeRangeReader.h>

namespace DB
{

using RangeReaders = std::vector<MergeTreeRangeReader>;

class MergeTreeReadersChain
{
public:
    MergeTreeReadersChain();
    explicit MergeTreeReadersChain(RangeReaders range_readers_);
    bool isInitialized() const { return is_initialized; }

    using ReadResult = MergeTreeRangeReader::ReadResult;
    ReadResult read(size_t max_rows, MarkRanges & ranges);

    size_t numReadRowsInCurrentGranule() const;
    size_t numPendingRowsInCurrentGranule() const;
    size_t numRowsInCurrentGranule() const;
    size_t currentMark() const;

    Block getSampleBlock() const;
    bool isCurrentRangeFinished() const;

private:
    void executePrewhereActions(MergeTreeRangeReader & reader, ReadResult & result, const Block & previous_header, bool is_last_reader);

    RangeReaders range_readers;

    bool is_initialized = false;
    LoggerPtr log;
};

};
