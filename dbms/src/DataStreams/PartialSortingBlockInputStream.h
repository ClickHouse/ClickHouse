#pragma once

#include <Core/SortDescription.h>

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class PartialSortingBlockInputStream : public IProfilingBlockInputStream
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingBlockInputStream(BlockInputStreamPtr input_, SortDescription & description_, size_t limit_ = 0)
        : description(description_), limit(limit_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "PartialSorting"; }

    String getID() const override
    {
        std::stringstream res;
        res << "PartialSorting(" << children.back()->getID();

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ")";
        return res.str();
    }

    bool isGroupedOutput() const override { return true; }
    bool isSortedOutput() const override { return true; }
    const SortDescription & getSortDescription() const override { return description; }

protected:
    Block readImpl() override;

private:
    SortDescription description;
    size_t limit;
};

}
