#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/PODArray.h>

namespace DB
{

class ReverseBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// Empty columns_ means all collumns.
    ReverseBlockInputStream(const BlockInputStreamPtr & input)
    {
    	children.push_back(input);
    }

    String getName() const override { return "Reverse"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override
    {
    	while (1)
    	{
    		auto res = children.back()->read();

    		if (!res)
    			return Block();

    		PaddedPODArray<size_t> perm;

    		for (int i = res.rows() - 1; i >= 0; --i)
    			perm.push_back(static_cast<size_t>(i));

    		for (auto it = res.begin(); it != res.end(); ++it)
    			it->column->permute(perm, 0);

    		return res;
    	}

    }
};

}