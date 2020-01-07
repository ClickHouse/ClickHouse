#include <queue>
#include <iomanip>
#include <sstream>

#include <DataStreams/MergingSortedBlockInputStream.h>
#include <DataStreams/ColumnGathererStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


MergingSortedBlockInputStream::MergingSortedBlockInputStream(
    const BlockInputStreams & inputs_, const SortDescription & description_,
    size_t max_block_size_, UInt64 limit_, UInt64 offset_, WriteBuffer * out_row_sources_buf_, bool quiet_, bool average_block_sizes_)
    : description(description_), max_block_size(max_block_size_), limit(limit_), offset(offset_), quiet(quiet_)
    , average_block_sizes(average_block_sizes_), source_blocks(inputs_.size())
    , cursors(inputs_.size()), out_row_sources_buf(out_row_sources_buf_)
{
    children.insert(children.end(), inputs_.begin(), inputs_.end());
    header = children.at(0)->getHeader();
    num_columns = header.columns();
}

void MergingSortedBlockInputStream::init(MutableColumns & merged_columns)
{
    /// Read the first blocks, initialize the queue.
    if (first)
    {
        first = false;

        for (size_t i = 0; i < source_blocks.size(); ++i)
        {
            SharedBlockPtr & shared_block_ptr = source_blocks[i];

            if (shared_block_ptr.get())
                continue;

            shared_block_ptr = new detail::SharedBlock(children[i]->read());

            const size_t rows = shared_block_ptr->rows();

            if (rows == 0)
                continue;

            if (expected_block_size < rows)
                expected_block_size = std::min(rows, max_block_size);

            cursors[i] = SortCursorImpl(*shared_block_ptr, description, i);
            shared_block_ptr->all_columns = cursors[i].all_columns;
            shared_block_ptr->sort_columns = cursors[i].sort_columns;
            has_collation |= cursors[i].has_collation;
        }

        if (has_collation)
            queue_with_collation = SortingHeap<SortCursorWithCollation>(cursors);
        else
            queue_without_collation = SortingHeap<SortCursor>(cursors);
    }

    /// Let's check that all source blocks have the same structure.
    for (const SharedBlockPtr & shared_block_ptr : source_blocks)
    {
        if (!*shared_block_ptr)
            continue;

        assertBlocksHaveEqualStructure(*shared_block_ptr, header, getName());
    }

    merged_columns.resize(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        merged_columns[i] = header.safeGetByPosition(i).column->cloneEmpty();
        merged_columns[i]->reserve(expected_block_size);
    }
}


Block MergingSortedBlockInputStream::readImpl()
{
    if (finished)
        return {};

    if (children.size() == 1)
        return children[0]->read();

    MutableColumns merged_columns;

    init(merged_columns);
    if (merged_columns.empty())
        return {};

    if (has_collation)
        merge(merged_columns, queue_with_collation);
    else
        merge(merged_columns, queue_without_collation);

    return header.cloneWithColumns(std::move(merged_columns));
}


template <typename TSortCursor>
void MergingSortedBlockInputStream::fetchNextBlock(const TSortCursor & current, SortingHeap<TSortCursor> & queue)
{
    size_t order = current->order;
    size_t size = cursors.size();

    if (order >= size || &cursors[order] != current.impl)
        throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

    while (true)
    {
        source_blocks[order] = new detail::SharedBlock(children[order]->read());    /// intrusive ptr

        if (!*source_blocks[order])
        {
            queue.removeTop();
            break;
        }

        if (source_blocks[order]->rows())
        {
            cursors[order].reset(*source_blocks[order]);
            queue.replaceTop(&cursors[order]);

            source_blocks[order]->all_columns = cursors[order].all_columns;
            source_blocks[order]->sort_columns = cursors[order].sort_columns;
            break;
        }
    }
}


template <typename TSortCursor>
void MergingSortedBlockInputStream::skipBlock(typename SortingHeap<TSortCursor>::Container::iterator it, SortingHeap<TSortCursor> & queue)
{
    size_t order = (*it)->order;
    size_t size = cursors.size();

    if (order >= size || &cursors[order] != it->impl)
        throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

    while (true)
    {
        source_blocks[order] = new detail::SharedBlock(children[order]->read());    /// intrusive ptr

        if (!*source_blocks[order])
        {
            queue.container().erase(it);
            break;
        }

        if (source_blocks[order]->rows())
        {
            cursors[order].reset(*source_blocks[order]);
            *it = TSortCursor(&cursors[order]);

            source_blocks[order]->all_columns = cursors[order].all_columns;
            source_blocks[order]->sort_columns = cursors[order].sort_columns;
            break;
        }
    }

    queue.rebuild();
}


bool MergingSortedBlockInputStream::MergeStopCondition::checkStop() const
{
    if (!count_average)
        return sum_rows_count == max_block_size;

    if (sum_rows_count == 0)
        return false;

    size_t average = sum_blocks_granularity / sum_rows_count;
    return sum_rows_count >= average;
}


template <typename TSortingHeap>
void MergingSortedBlockInputStream::merge(MutableColumns & merged_columns, TSortingHeap & queue)
{
    size_t merged_rows = 0;

    MergeStopCondition stop_condition(average_block_sizes, max_block_size);

    /** Increase row counters.
      * Return true if it's time to finish generating the current data block.
      */
    auto count_row_and_check_limit = [&, this](size_t current_granularity)
    {
        ++total_merged_rows;
        if (limit && total_merged_rows == limit)
        {
//            std::cerr << "Limit reached\n";
            cancel(false);
            finished = true;
            return true;
        }

        ++merged_rows;
        stop_condition.addRowWithGranularity(current_granularity);
        return stop_condition.checkStop();
    };

    /// Take rows in required order and put them into `merged_columns`, while the number of rows are no more than `max_block_size`
    while (queue.isValid())
    {
        /** "Fast Forward" optimization for LIMIT with OFFSET.
          *
          * If there is offset to skip and if current blocks in all cursors have less or equal number of rows in total than the offset,
          * we can select the block that is "smallest" according to it's last row than other blocks
          * (it means - if we iterate 'offset' number of rows, this block will be processed earlier than other blocks)
          * and just skip it, yielding a block with the same size and default values.
          *
          * This allows to avoid tons of comparisons.
          *
          * Note that 'out_row_sources_buf' is not filled in this branch because it cannot be used with non-zero 'offset'.
          */
        if (!merged_rows && total_merged_rows < offset)
        {
            size_t total_rows_under_cursors = 0;
            for (size_t i = 0, size = queue.size(); i < size; ++i)
                total_rows_under_cursors += queue.container()[i]->rows;

//            std::cerr << total_merged_rows << ", " << offset << ", " << total_rows_under_cursors << ", " << queue.size() << "\n";

            if (total_merged_rows + total_rows_under_cursors < offset)
            {
                auto smallest_cursor = std::min_element(
                    queue.container().begin(), queue.container().end(),
                    [](const auto & a, const auto & b){ return a.willBeFinishedEarlier(b); });

                size_t num_rows_to_skip = (*smallest_cursor)->rows;

//                std::cerr << "Fast-forward " << num_rows_to_skip << " rows.\n";

                for (size_t i = 0; i < num_columns; ++i)
                    merged_columns[i] = merged_columns[i]->cloneResized(num_rows_to_skip);

                skipBlock(smallest_cursor, queue);
                total_merged_rows += num_rows_to_skip;
                return;
            }
            else
            {
//                std::cerr << "Stop fast forward.\n";

                /// Stop "fast forward".
                offset = 0;
            }
        }

        auto current = queue.current();
        size_t current_block_granularity = current->rows;

        /** And what if the block is totally less or equal than the rest for the current cursor?
          * Or is there only one data source left in the queue? Then you can take the entire block on current cursor.
          */
        if (current->isFirst()
            && (queue.size() == 1
                || (queue.size() >= 2 && current.totallyLessOrEquals(queue.nextChild()))))
        {
//            std::cerr << "current block is totally less or equals\n";

            /// If there are already data in the current block, we first return it. We'll get here again the next time we call the merge function.
            if (merged_rows != 0)
            {
                //std::cerr << "merged rows is non-zero\n";
                return;
            }

            /// Actually, current->order stores source number (i.e. cursors[current->order] == current)
            size_t source_num = current->order;

            if (source_num >= cursors.size())
                throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

            for (size_t i = 0; i < num_columns; ++i)
                merged_columns[i] = (std::move(*current->all_columns[i])).mutate();

//            std::cerr << "copied columns\n";

            merged_rows = merged_columns.at(0)->size();

            /// Limit output
            if (limit && total_merged_rows + merged_rows > limit)
            {
                merged_rows = limit - total_merged_rows;
                for (size_t i = 0; i < num_columns; ++i)
                {
                    auto & column = merged_columns[i];
                    column = (*column->cut(0, merged_rows)).mutate();
                }

                cancel(false);
                finished = true;
            }

            /// Write order of rows for other columns
            /// this data will be used in grather stream
            if (out_row_sources_buf)
            {
                RowSourcePart row_source(source_num);
                for (size_t i = 0; i < merged_rows; ++i)
                    out_row_sources_buf->write(row_source.data);
            }

//            std::cerr << "fetching next block\n";

            total_merged_rows += merged_rows;
            fetchNextBlock(current, queue);
            return;
        }

//        std::cerr << "total_merged_rows: " << total_merged_rows << ", merged_rows: " << merged_rows << "\n";
//        std::cerr << "Inserting row\n";
        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        if (out_row_sources_buf)
        {
            /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
            RowSourcePart row_source(current->order);
            out_row_sources_buf->write(row_source.data);
        }

        if (!current->isLast())
        {
//            std::cerr << "moving to next row\n";
            queue.next();
        }
        else
        {
            /// We get the next block from the corresponding source, if there is one.
//            std::cerr << "It was last row, fetching next block\n";
            fetchNextBlock(current, queue);
        }

        if (count_row_and_check_limit(current_block_granularity))
            return;
    }

    /// We have read all data. Ask childs to cancel providing more data.
    cancel(false);
    finished = true;
}


void MergingSortedBlockInputStream::readSuffixImpl()
{
     if (quiet)
         return;

    const BlockStreamProfileInfo & profile_info = getProfileInfo();
    double seconds = profile_info.total_stopwatch.elapsedSeconds();

    std::stringstream message;
    message << std::fixed << std::setprecision(2)
        << "Merge sorted " << profile_info.blocks << " blocks, " << profile_info.rows << " rows"
        << " in " << seconds << " sec.";

    if (seconds)
        message << ", "
        << profile_info.rows / seconds << " rows/sec., "
        << profile_info.bytes / 1000000.0 / seconds << " MB/sec.";

    LOG_DEBUG(log, message.str());
}

}
