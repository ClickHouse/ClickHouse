#pragma once
#include <common/logger_useful.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/SortDescription.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <queue>

namespace DB
{

/// Collapses the same rows with the opposite sign roughly like CollapsingSortedBlockInputStream.
/// Outputs the rows in random order (the input streams must still be ordered).
/// Outputs only rows with a positive sign.
class CollapsingFinalBlockInputStream : public IBlockInputStream
{
public:
    CollapsingFinalBlockInputStream(
        const BlockInputStreams & inputs,
        const SortDescription & description_,
        const String & sign_column_name_)
        : description(description_), sign_column_name(sign_column_name_)
    {
        children.insert(children.end(), inputs.begin(), inputs.end());
    }

    ~CollapsingFinalBlockInputStream() override;

    String getName() const override { return "CollapsingFinal"; }

    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    struct MergingBlock;
    using BlockPlainPtrs = std::vector<MergingBlock*>;

    struct MergingBlock : boost::noncopyable
    {
        MergingBlock(const Block & block_,
                     size_t stream_index_,
                     const SortDescription & desc,
                     const String & sign_column_name,
                     BlockPlainPtrs * output_blocks)
            : block(block_), stream_index(stream_index_), output_blocks(output_blocks)
        {
            sort_columns.resize(desc.size());
            for (size_t i = 0; i < desc.size(); ++i)
            {
                size_t column_number = !desc[i].column_name.empty()
                    ? block.getPositionByName(desc[i].column_name)
                    : desc[i].column_number;

                sort_columns[i] = block.safeGetByPosition(column_number).column.get();
            }

            const IColumn * sign_icolumn = block.getByName(sign_column_name).column.get();

            sign_column = typeid_cast<const ColumnInt8 *>(sign_icolumn);

            if (!sign_column)
                throw Exception("Sign column must have type Int8", ErrorCodes::BAD_TYPE_OF_FIELD);

            rows = sign_column->size();
            /// Filled entirely with zeros. Then `1` are set in the positions of the rows to be left.
            filter.resize_fill(rows);
        }

        Block block;

        /// Rows with the same key will be sorted in ascending order of stream_index.
        size_t stream_index;
        size_t rows;

        /// Which rows should be left. Filled when the threads merge.
        IColumn::Filter filter;

        /// Point to `block`.
        ColumnRawPtrs sort_columns;
        const ColumnInt8 * sign_column;

        /// When it reaches zero, the block can be outputted in response.
        int refcount = 0;

        /// Where to put the block when it is ready to be outputted in response.
        BlockPlainPtrs * output_blocks;
    };

    /// When deleting the last block reference, adds a block to `output_blocks`.
    class MergingBlockPtr
    {
    public:
        MergingBlockPtr() : ptr() {}

        explicit MergingBlockPtr(MergingBlock * ptr_) : ptr(ptr_)
        {
            if (ptr)
                ++ptr->refcount;
        }

        MergingBlockPtr(const MergingBlockPtr & rhs) : ptr(rhs.ptr)
        {
            if (ptr)
                ++ptr->refcount;
        }

        MergingBlockPtr & operator=(const MergingBlockPtr & rhs)
        {
            destroy();
            ptr = rhs.ptr;
            if (ptr)
                ++ptr->refcount;
            return *this;
        }

        ~MergingBlockPtr()
        {
            destroy();
        }

        /// Zero the pointer and do not add a block to output_blocks.
        void cancel()
        {
            if (ptr)
            {
                --ptr->refcount;
                if (!ptr->refcount)
                    delete ptr;
                ptr = nullptr;
            }
        }

        MergingBlock & operator*() const { return *ptr; }
        MergingBlock * operator->() const { return ptr; }
        operator bool() const { return !!ptr; }
        bool operator!() const { return !ptr; }

    private:
        MergingBlock * ptr;

        void destroy()
        {
            if (ptr)
            {
                --ptr->refcount;
                if (!ptr->refcount)
                {
                    if (std::uncaught_exceptions())
                        delete ptr;
                    else
                        ptr->output_blocks->push_back(ptr);
                }
                ptr = nullptr;
            }
        }
    };

    struct Cursor
    {
        MergingBlockPtr block;
        size_t pos = 0;

        Cursor() {}
        explicit Cursor(const MergingBlockPtr & block_, size_t pos_ = 0) : block(block_), pos(pos_) {}

        bool operator< (const Cursor & rhs) const
        {
            for (size_t i = 0; i < block->sort_columns.size(); ++i)
            {
                int res = block->sort_columns[i]->compareAt(pos, rhs.pos, *(rhs.block->sort_columns[i]), 1);
                if (res > 0)
                    return true;
                if (res < 0)
                    return false;
            }

            return block->stream_index > rhs.block->stream_index;
        }

        /// Not consistent with operator< : does not consider order.
        bool equal(const Cursor & rhs) const
        {
            if (!block || !rhs.block)
                return false;

            for (size_t i = 0; i < block->sort_columns.size(); ++i)
            {
                int res = block->sort_columns[i]->compareAt(pos, rhs.pos, *(rhs.block->sort_columns[i]), 1);
                if (res != 0)
                    return false;
            }

            return true;
        }

        Int8 getSign()
        {
            return block->sign_column->getData()[pos];
        }

        /// Indicates that this row should be outputted in response.
        void addToFilter()
        {
            block->filter[pos] = 1;
        }

        bool isLast()
        {
            return pos + 1 == block->rows;
        }

        void next()
        {
            ++pos;
        }
    };

    using Queue = std::priority_queue<Cursor>;

    const SortDescription description;
    String sign_column_name;

    Logger * log = &Logger::get("CollapsingFinalBlockInputStream");

    bool first = true;

    BlockPlainPtrs output_blocks;

    Queue queue;

    Cursor previous;                    /// The current primary key.
    Cursor last_positive;               /// The last positive row for the current primary key.

    size_t count_positive = 0;          /// The number of positive rows for the current primary key.
    size_t count_negative = 0;          /// The number of negative rows for the current primary key.
    bool last_is_positive = false;      /// true if the last row for the current primary key is positive.

    size_t count_incorrect_data = 0;    /// To prevent too many error messages from writing to the log.

    /// Count the number of blocks fetched and outputted.
    size_t blocks_fetched = 0;
    size_t blocks_output = 0;

    void fetchNextBlock(size_t input_index);
    void commitCurrent();

    void reportBadCounts();
    void reportBadSign(Int8 sign);
};

}
