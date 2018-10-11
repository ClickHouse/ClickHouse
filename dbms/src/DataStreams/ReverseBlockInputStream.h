#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/PODArray.h>

namespace DB
{

/// The order of rows in each block is reversed.
class ReverseBlockInputStream : public IProfilingBlockInputStream
{
public:
    ReverseBlockInputStream(const BlockInputStreamPtr & input)
    {
        children.push_back(input);
    }

    String getName() const override { return "Reverse"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override
    {
        auto res = children.back()->read();

        if (!res)
            return Block();

        PaddedPODArray<size_t> perm;

        for (ssize_t i = res.rows() - 1; i >= 0; --i)
            perm.push_back(i);

        for (auto it = res.begin(); it != res.end(); ++it)
            it->column = it->column->permute(perm, 0);

        return res;
    }
};

}
