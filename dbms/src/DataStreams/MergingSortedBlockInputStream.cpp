#include <queue>
#include <iomanip>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
}


MergingSortedBlockInputStream::MergingSortedBlockInputStream(
        BlockInputStreams & inputs_, const SortDescription & description_,
        size_t max_block_size_, size_t limit_, WriteBuffer * out_row_sources_buf_, bool quiet_)
    : description(description_), max_block_size(max_block_size_), limit(limit_), quiet(quiet_)
    , source_blocks(inputs_.size()), cursors(inputs_.size()), out_row_sources_buf(out_row_sources_buf_)
{
    children.insert(children.end(), inputs_.begin(), inputs_.end());
}

String MergingSortedBlockInputStream::getID() const
{
    std::stringstream res;
    res << "MergingSorted(";

    Strings children_ids(children.size());
    for (size_t i = 0; i < children.size(); ++i)
        children_ids[i] = children[i]->getID();

    /// The order does not matter.
    std::sort(children_ids.begin(), children_ids.end());

    for (size_t i = 0; i < children_ids.size(); ++i)
        res << (i == 0 ? "" : ", ") << children_ids[i];

    for (size_t i = 0; i < description.size(); ++i)
        res << ", " << description[i].getID();

    res << ")";
    return res.str();
}

void MergingSortedBlockInputStream::init(Block & merged_block, ColumnPlainPtrs & merged_columns)
{
    /// Read the first blocks, initialize the queue.
    if (first)
    {
        first = false;

        size_t i = 0;
        for (auto it = source_blocks.begin(); it != source_blocks.end(); ++it, ++i)
        {
            SharedBlockPtr & shared_block_ptr = *it;

            if (shared_block_ptr.get())
                continue;

            shared_block_ptr = new detail::SharedBlock(children[i]->read());

            const size_t rows = shared_block_ptr->rows();

            if (rows == 0)
                continue;

            if (!num_columns)
                num_columns = shared_block_ptr->columns();

            if (expected_block_size < rows)
                expected_block_size = std::min(rows, max_block_size);

            cursors[i] = SortCursorImpl(*shared_block_ptr, description, i);
            has_collation |= cursors[i].has_collation;
        }

        if (has_collation)
            initQueue(queue_with_collation);
        else
            initQueue(queue);
    }

    /// Initialize the result.

    /// We clone the structure of the first non-empty source block.
    {
        auto it = source_blocks.cbegin();
        for (; it != source_blocks.cend(); ++it)
        {
            const SharedBlockPtr & shared_block_ptr = *it;

            if (*shared_block_ptr)
            {
                merged_block = shared_block_ptr->cloneEmpty();
                break;
            }
        }

        /// If all the input blocks are empty.
        if (it == source_blocks.cend())
            return;
    }

    /// Let's check that all source blocks have the same structure.
    for (auto it = source_blocks.cbegin(); it != source_blocks.cend(); ++it)
    {
        const SharedBlockPtr & shared_block_ptr = *it;

        if (!*shared_block_ptr)
            continue;

        size_t src_columns = shared_block_ptr->columns();
        size_t dst_columns = merged_block.columns();

        if (src_columns != dst_columns)
            throw Exception("Merging blocks has different number of columns ("
                + toString(src_columns) + " and " + toString(dst_columns) + ")",
                ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

        for (size_t i = 0; i < src_columns; ++i)
        {
            if (shared_block_ptr->safeGetByPosition(i).name != merged_block.safeGetByPosition(i).name
                || shared_block_ptr->safeGetByPosition(i).type->getName() != merged_block.safeGetByPosition(i).type->getName()
                || shared_block_ptr->safeGetByPosition(i).column->getName() != merged_block.safeGetByPosition(i).column->getName())
            {
                throw Exception("Merging blocks has different names or types of columns:\n"
                    + shared_block_ptr->dumpStructure() + "\nand\n" + merged_block.dumpStructure(),
                    ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
            }
        }
    }

    for (size_t i = 0; i < num_columns; ++i)
    {
        merged_columns.emplace_back(merged_block.safeGetByPosition(i).column.get());
        merged_columns.back()->reserve(expected_block_size);
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
        return Block();

    if (children.size() == 1)
        return children[0]->read();

    Block merged_block;
    ColumnPlainPtrs merged_columns;

    init(merged_block, merged_columns);
    if (merged_columns.empty())
        return Block();

    if (has_collation)
        merge(merged_block, merged_columns, queue_with_collation);
    else
        merge(merged_block, merged_columns, queue);

    return merged_block;
}

template <typename TSortCursor>
void MergingSortedBlockInputStream::merge(Block & merged_block, ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue)
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
            cancel();
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

    /// Take rows in required order and put them into `merged_block`, while the rows are no more than `max_block_size`
    while (!queue.empty())
    {
        TSortCursor current = queue.top();
        queue.pop();

        while (true)
        {
            /** And what if the block is smaller or equal than the rest for the current cursor?
              * Or is there only one data source left in the queue? Then you can take the entire block of current cursor.
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
                    merged_block.getByPosition(i).column = source_blocks[source_num]->getByPosition(i).column;

    //            std::cerr << "copied columns\n";

                size_t merged_rows = merged_block.rows();

                if (limit && total_merged_rows + merged_rows > limit)
                {
                    merged_rows = limit - total_merged_rows;
                    for (size_t i = 0; i < num_columns; ++i)
                    {
                        auto & column = merged_block.getByPosition(i).column;
                        column = column->cut(0, merged_rows);
                    }

                    cancel();
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

    cancel();
    finished = true;
}


template <typename TSortCursor>
void MergingSortedBlockInputStream::fetchNextBlock(const TSortCursor & current, std::priority_queue<TSortCursor> & queue)
{
    size_t i = 0;
    size_t size = cursors.size();
    for (; i < size; ++i)
    {
        if (&cursors[i] == current.impl)
        {
            source_blocks[i] = new detail::SharedBlock(children[i]->read());
            if (*source_blocks[i])
            {
                cursors[i].reset(*source_blocks[i]);
                queue.push(TSortCursor(&cursors[i]));
            }

            break;
        }
    }

    if (i == size)
        throw Exception("Logical error in MergingSortedBlockInputStream", ErrorCodes::LOGICAL_ERROR);
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
