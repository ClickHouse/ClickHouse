#pragma once

#include <Core/SortDescription.h>

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class PartialSortingBlockInputStream : public IBlockInputStream
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingBlockInputStream(const BlockInputStreamPtr & input_, SortDescription & description_, UInt64 limit_ = 0)
        : description(description_), limit(limit_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "PartialSorting"; }
    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    UInt64 limit;
};

}
