#include "ReverseBlockInputStream.h"

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

        PaddedPODArray<size_t> permutation;

        for (size_t i = 0; i < result_block.rows(); ++i)
        {
            permutation.emplace_back(result_block.rows() - 1 - i);
        }

        for (auto iter = result_block.begin(); iter != result_block.end(); ++iter)
        {
            iter->column = iter->column->permute(permutation, 0);
        }

        return result_block;
    }

} // namespace DB
