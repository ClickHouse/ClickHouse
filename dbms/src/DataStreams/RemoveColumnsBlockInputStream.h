#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Columns/ColumnConst.h>



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

    String getID() const override
    {
        std::stringstream res;
        res << "RemoveColumns(" << children.back()->getID();

        for (const auto & it : columns_to_remove)
            res << ", " << it;

        res << ")";
        return res.str();
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
