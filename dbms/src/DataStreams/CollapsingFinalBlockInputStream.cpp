#include <DataStreams/CollapsingFinalBlockInputStream.h>

/// Maximum number of messages about incorrect data in the log.
#define MAX_ERROR_MESSAGES 10


namespace DB
{

CollapsingFinalBlockInputStream::~CollapsingFinalBlockInputStream()
{
    /// You must cancel all `MergingBlockPtr` so that they do not try to put blocks in `output_blocks`.
    previous.block.cancel();
    last_positive.block.cancel();

    while (!queue.empty())
    {
        Cursor c = queue.top();
        queue.pop();
        c.block.cancel();
    }

    for (size_t i = 0; i < output_blocks.size(); ++i)
        delete output_blocks[i];
}

void CollapsingFinalBlockInputStream::reportBadCounts()
{
    /// With inconsistent data, this is an unavoidable error that can not be easily fixed by admins. Therefore Warning.
    LOG_WARNING(log, "Incorrect data: number of rows with sign = 1 (" << count_positive
        << ") differs with number of rows with sign = -1 (" << count_negative
        << ") by more than one");
}

void CollapsingFinalBlockInputStream::reportBadSign(Int8 sign)
{
    LOG_ERROR(log, "Invalid sign: " << static_cast<int>(sign));
}

void CollapsingFinalBlockInputStream::fetchNextBlock(size_t input_index)
{
    BlockInputStreamPtr stream = children[input_index];
    Block block = stream->read();
    if (!block)
        return;
    MergingBlockPtr merging_block(new MergingBlock(block, input_index, description, sign_column_name, &output_blocks));
    ++blocks_fetched;
    queue.push(Cursor(merging_block));
}

void CollapsingFinalBlockInputStream::commitCurrent()
{
    if (count_positive || count_negative)
    {
        if (count_positive >= count_negative && last_is_positive)
        {
            last_positive.addToFilter();
        }

        if (!(count_positive == count_negative || count_positive + 1 == count_negative || count_positive == count_negative + 1))
        {
            if (count_incorrect_data < MAX_ERROR_MESSAGES)
                reportBadCounts();
            ++count_incorrect_data;
        }

        last_positive = Cursor();
        previous = Cursor();
    }

    count_negative = 0;
    count_positive = 0;
}

Block CollapsingFinalBlockInputStream::readImpl()
{
    if (first)
    {
        for (size_t i = 0; i < children.size(); ++i)
            fetchNextBlock(i);

        first = false;
    }

    /// We will create blocks for the answer until we get a non-empty block.
    while (true)
    {
        while (!queue.empty() && output_blocks.empty())
        {
            Cursor current = queue.top();
            queue.pop();

            bool has_next = !queue.empty();
            Cursor next = has_next ? queue.top() : Cursor();

            /// We will advance in the current block, not using the queue, as long as possible.
            while (true)
            {
                if (!current.equal(previous))
                {
                    commitCurrent();
                    previous = current;
                }

                Int8 sign = current.getSign();
                if (sign == 1)
                {
                    last_positive = current;
                    last_is_positive = true;
                    ++count_positive;
                }
                else if (sign == -1)
                {
                    last_is_positive = false;
                    ++count_negative;
                }
                else
                    reportBadSign(sign);

                if (current.isLast())
                {
                    fetchNextBlock(current.block->stream_index);

                    /// All streams are over. We'll process the last key.
                    if (!has_next)
                        commitCurrent();

                    break;
                }
                else
                {
                    current.next();

                    if (has_next && !(next < current))
                    {
                        queue.push(current);
                        break;
                    }
                }
            }
        }

        /// End of the stream.
        if (output_blocks.empty())
        {
            if (blocks_fetched != blocks_output)
                LOG_ERROR(log, "Logical error: CollapsingFinalBlockInputStream has output " << blocks_output << " blocks instead of " << blocks_fetched);

            return Block();
        }

        MergingBlock * merging_block = output_blocks.back();
        Block block = merging_block->block;

        for (size_t i = 0; i < block.columns(); ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(merging_block->filter, -1);

        output_blocks.pop_back();
        delete merging_block;

        ++blocks_output;

        if (block)
            return block;
    }
}

}
