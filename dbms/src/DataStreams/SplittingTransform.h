#pragma once

#include <queue>
#include <Core/Block.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_LIST_OF_COLUMNS_PASSED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

class SplittingTransform
{
public:
    SplittingTransform() {}

    void add(MutableColumns && columns)
    {
        assertColumnsHasEqualRows(columns);
        blocks_queue.push(std::move(columns));
    }

    MutableColumns get(size_t preferable_size)
    {
        if (blocks_queue.empty())
            return {};
        
        auto & columns = blocks_queue.front();
        size_t rows = columns[0]->size();

        MutableColumns result;
        if (rows <= preferable_size && current_offset == 0)
        {
            result = std::move(columns);
            blocks_queue.pop();
            current_offset = 0;
        }
        else
        {
            result.reserve(columns.size());
            size_t result_size = std::min(rows - current_offset, preferable_size);
            for (const auto & column : columns)
                result.push_back((*column->cut(current_offset, result_size)).mutate());

            current_offset += result_size;
        }

        if (current_offset == rows)
        {
            blocks_queue.pop();
            current_offset = 0;
        }

        return result;
    }

    MutableColumns addAndGet(MutableColumns && columns, size_t preferable_size)
    {
        add(std::move(columns));
        return get(preferable_size);
    }

private:

    void assertColumnsHasEqualRows(const MutableColumns & columns)
    {
        if (columns.empty())
            throw Exception("Empty list of columns passed", ErrorCodes::EMPTY_LIST_OF_COLUMNS_PASSED);

        size_t rows = 0;
        for (const auto & column : columns)
        {
            if (!rows)
                rows = column->size();
            else if (column->size() != rows)
                throw Exception("Sizes of columns doesn't match", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
        }
    }

    // size_t max_block_size_rows = 0;
    // size_t max_block_size_bytes = 0;
    std::queue<MutableColumns> blocks_queue;
    size_t current_offset = 0;
};

}
