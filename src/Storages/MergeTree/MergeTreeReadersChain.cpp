#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadersChain::MergeTreeReadersChain(RangeReaders range_readers_)
    : range_readers(std::move(range_readers_))
    , is_initialized(true)
{
}

size_t MergeTreeReadersChain::numReadRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.front().numReadRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::numPendingRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.front().numPendingRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::numRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.back().numRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::currentMark() const
{
    return range_readers.empty() ? 0 : range_readers.back().currentMark();
}

Block MergeTreeReadersChain::getSampleBlock() const
{
    return range_readers.empty() ? Block{} : range_readers.back().getSampleBlock();
}

bool MergeTreeReadersChain::isCurrentRangeFinished() const
{
    return range_readers.empty() ? true : range_readers.front().isCurrentRangeFinished();
}

MergeTreeReadersChain::ReadResult MergeTreeReadersChain::read(size_t max_rows, MarkRanges & ranges)
{
    if (max_rows == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected at least 1 row to read, got 0.");

    if (range_readers.empty())
        return ReadResult{log};

    auto & first_reader = range_readers.front();
    auto read_result = first_reader.startReadingChain(max_rows, ranges);

    LOG_TEST(log, "First reader returned: {}, requested columns: {}", read_result.dumpInfo(), first_reader.getSampleBlock().dumpNames());

    if (read_result.num_rows != 0)
    {
        first_reader.executeActionsBeforePrewhere(read_result, read_result.columns, {}, read_result.num_rows);
        executePrewhereActions(first_reader, read_result, {}, range_readers.size() == 1);
    }

    for (size_t i = 1; i < range_readers.size(); ++i)
    {
        size_t num_read_rows = 0;
        auto columns = range_readers[i].continueReadingChain(read_result, num_read_rows);

        /// Even if number of read rows is 0 we need to apply all steps to produce a block with correct structure.
        /// It's also needed to properly advancing streams in later steps.
        if (read_result.num_rows == 0)
            continue;

        const auto & previous_header = range_readers[i - 1].getSampleBlock();

        if (!columns.empty())
        {
            /// If all requested columns are absent in part num_read_rows will be 0.
            /// In this case we need to use number of rows in the result to fill the default values and don't filter block.
            if (num_read_rows == 0)
                num_read_rows = read_result.num_rows;

            range_readers[i].executeActionsBeforePrewhere(read_result, columns, previous_header, num_read_rows);
            read_result.columns.reserve(read_result.columns.size() + columns.size());
            std::move(columns.begin(), columns.end(), std::back_inserter(read_result.columns));
        }

        executePrewhereActions(range_readers[i], read_result, previous_header, i + 1 == range_readers.size());
    }

    return read_result;
}

void MergeTreeReadersChain::executePrewhereActions(
    MergeTreeRangeReader & reader, ReadResult & result, const Block & previous_header, bool is_last_reader)
{
    reader.executePrewhereActionsAndFilterColumns(result, previous_header, is_last_reader);
    result.checkInternalConsistency();

    if (!result.can_return_prewhere_column_without_filtering && is_last_reader)
    {
        if (!result.filterWasApplied())
        {
            /// TODO: another solution might be to set all 0s from final filter into the prewhere column
            /// and not filter all the columns here but rely on filtering in WHERE.
            result.applyFilter(result.final_filter);
            result.checkInternalConsistency();
        }

        result.can_return_prewhere_column_without_filtering = true;
    }

    const auto & sample_block = reader.getSampleBlock();
    if (result.num_rows != 0 && result.columns.size() != sample_block.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of columns in result doesn't match number of columns in sample block, read_result: {}, sample block: {}",
            result.dumpInfo(), sample_block.dumpStructure());
}

}
