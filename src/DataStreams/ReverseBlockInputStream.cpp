#include "ReverseBlockInputStream.h"

#include <Common/PODArray.h>

namespace DB
{
    ReverseBlockInputStream::ReverseBlockInputStream(const BlockInputStreamPtr & input)
    {
        children.push_back(input);
    }

    String ReverseBlockInputStream::getName() const
    {
        return "Reverse";
    }

    Block ReverseBlockInputStream::getHeader() const
    {
        return children.at(0)->getHeader();
    }

    Block ReverseBlockInputStream::readImpl()
    {
        auto result_block = children.back()->read();

        if (!result_block)
        {
            return Block();
        }

        IColumn::Permutation permutation;

        size_t rows_size = result_block.rows();
        for (size_t i = 0; i < rows_size; ++i)
            permutation.emplace_back(rows_size - 1 - i);

        for (auto & block : result_block)
            block.column = block.column->permute(permutation, 0);

        return result_block;
    }
}
