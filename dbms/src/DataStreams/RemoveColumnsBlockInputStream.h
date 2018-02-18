#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>



namespace DB
{

/** Removes the specified columns from the block.
    */
class RemoveColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    RemoveColumnsBlockInputStream(
        BlockInputStreamPtr input_,
        const Names & columns_to_remove_)
    : columns_to_remove(columns_to_remove_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "RemoveColumns"; }

    Block getHeader() const override
    {
        Block res = children.back()->getHeader();

        for (const auto & it : columns_to_remove)
            if (res.has(it))
                res.erase(it);

        return res;
    }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return res;

        for (const auto & it : columns_to_remove)
            if (res.has(it))
                res.erase(it);

        return res;
    }

private:
    Names columns_to_remove;
};

}
