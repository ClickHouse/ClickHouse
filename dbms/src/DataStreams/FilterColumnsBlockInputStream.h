#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Columns/ColumnConst.h>
#include <iostream>

namespace DB
{

/// Removes columns other than columns_to_save_ from block
class FilterColumnsBlockInputStream : public IProfilingBlockInputStream
{
public:
    FilterColumnsBlockInputStream(
        BlockInputStreamPtr input_,
        const Names & columns_to_save_)
        : columns_to_save(columns_to_save_)
    {
        children.push_back(input_);
    }

    String getName() const override {
        return "FilterColumnsBlockInputStream";
    }

    String getID() const override
    {
        std::stringstream res;
        res << "FilterColumnsBlockInputStream(" << children.back()->getID();

        for (const auto & it : columns_to_save)
            res << ", " << it;

        res << ")";
        return res.str();
    }

protected:
    Block readImpl() override
    {
        Block block = children.back()->read();

        if (!block)
            return block;
        Block filtered;

        for (const auto & it : columns_to_save)
            filtered.insert(std::move(block.getByName(it)));


        return filtered;
    }

private:
    Names columns_to_save;
};

}
