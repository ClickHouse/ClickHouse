#include <queue>
#include <iomanip>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}


MergingSortedBlockInputStream::MergingSortedBlockInputStream(
    const BlockInputStreams & inputs_, const SortDescription & description_,
    size_t max_block_size_, size_t limit_, WriteBuffer * out_row_sources_buf_, bool quiet_)
    : description(description_), max_block_size(max_block_size_), limit(limit_), quiet(quiet_)
    , source_blocks(inputs_.size()), cursors(inputs_.size()), out_row_sources_buf(out_row_sources_buf_)
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
            initQueue(queue_with_collation);
        else
            initQueue(queue_without_collation);
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


template <typename TSortCursor>
void MergingSortedBlockInputStream::initQueue(std::priority_queue<TSortCursor> & queue)
{
    for (size_t i = 0; i < cursors.size(); ++i)
        if (!cursors[i].empty())
            queue.push(TSortCursor(&cursors[i]));
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
void MergingSortedBlockInputStream::fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue)
{
    size_t order = current.impl->order;
    size_t size = cursors.size();

    if (order >= size || &cursors[order] != current.impl)
        throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

    source_blocks[order] = new detail::SharedBlock(children[order]->read());
    if (*source_blocks[order])
    {
        cursors[order].reset(*source_blocks[order]);
        queue.push(TSortCursor(&cursors[order]));
        source_blocks[order]->all_columns = cursors[order].all_columns;
        source_blocks[order]->sort_columns = cursors[order].sort_columns;
    }
}

template
void MergingSortedBlockInputStream::fetchNextBlock<SortCursor>(const SortCursor & current, std::priority_queue<SortCursor> & queue);

template
void MergingSortedBlockInputStream::fetchNextBlock<SortCursorWithCollation>(const SortCursorWithCollation & current, std::priority_queue<SortCursorWithCollation> & queue);


template <typename TSortCursor>
void MergingSortedBlockInputStream::merge(MutableColumns & merged_columns, std::priority_queue<TSortCursor> & queue)
{
    size_t merged_rows = 0;

    /** Increase row counters.
      * Return true if it's time to finish generating the current data block.
      */
    auto count_row_and_check_limit = [&, this]()
    {
        ++total_merged_rows;
        if (limit && total_merged_rows == limit)
        {
    //        std::cerr << "Limit reached\n";
            cancel(false);
            finished = true;
            return true;
        }

        ++merged_rows;
        if (merged_rows == max_block_size)
        {
    //        std::cerr << "max_block_size reached\n";
            return true;
        }

        return false;
    };

    /// Take rows in required order and put them into `merged_columns`, while the rows are no more than `max_block_size`
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        while (true)
        {
            /** And what if the block is totally less or equal than the rest for the current cursor?
              * Or is there only one data source left in the queue? Then you can take the entire block on current cursor.
              */
            if (current.impl->isFirst() && (queue.empty() || current.totallyLessOrEquals(queue.top())))
            {
    //            std::cerr << "current block is totally less or equals\n";

                /// If there are already data in the current block, we first return it. We'll get here again the next time we call the merge function.
                if (merged_rows != 0)
                {
    //                std::cerr << "merged rows is non-zero\n";
                    queue.push(current);
                    return;
                }

                /// Actually, current.impl->order stores source number (i.e. cursors[current.impl->order] == current.impl)
                size_t source_num = current.impl->order;

                if (source_num >= cursors.size())
                    throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);

                for (size_t i = 0; i < num_columns; ++i)
                    merged_columns[i] = (*std::move(source_blocks[source_num]->getByPosition(i).column)).mutate();

    //            std::cerr << "copied columns\n";

                merged_rows = merged_columns.at(0)->size();

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
                RowSourcePart row_source(current.impl->order);
                out_row_sources_buf->write(row_source.data);
            }

            if (!current->isLast())
            {
    //            std::cerr << "moving to next row\n";
                current->next();

                if (queue.empty() || !(current.greater(queue.top())))
                {
                    if (count_row_and_check_limit())
                    {
    //                    std::cerr << "pushing back to queue\n";
                        queue.push(current);
                        return;
                    }

                    /// Do not put the cursor back in the queue, but continue to work with the current cursor.
    //                std::cerr << "current is still on top, using current row\n";
                    continue;
                }
                else
                {
    //                std::cerr << "next row is not least, pushing back to queue\n";
                    queue.push(current);
                }
            }
            else
            {
                /// We get the next block from the corresponding source, if there is one.
    //            std::cerr << "It was last row, fetching next block\n";
                fetchNextBlock(current, queue);
            }

            break;
        }

        if (count_row_and_check_limit())
            return;
    }

    cancel(false);
    finished = true;
}


void MergingSortedBlockInputStream::readSuffixImpl()
{
     if (quiet)
         return;

    const BlockStreamProfileInfo & profile_info = getProfileInfo();
    double seconds = profile_info.total_stopwatch.elapsedSeconds();
    LOG_DEBUG(log, std::fixed << std::setprecision(2)
        << "Merge sorted " << profile_info.blocks << " blocks, " << profile_info.rows << " rows"
        << " in " << seconds << " sec., "
        << profile_info.rows / seconds << " rows/sec., "
        << profile_info.bytes / 1000000.0 / seconds << " MB/sec.");
}

}
