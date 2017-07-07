#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Columns/ColumnConst.h>
#include <iostream>

namespace DB
{

/// Removes columns other than columns_to_save_ from block,
///  and reorders columns as in columns_to_save_.
/// Functionality is similar to ExpressionBlockInputStream with ExpressionActions containing PROJECT action.
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

    String getName() const override
    {
        return "FilterColumnsBlockInputStream";
    }

    String getID() const override;

protected:
    Block readImpl() override;

private:
    Names columns_to_save;
};

}
