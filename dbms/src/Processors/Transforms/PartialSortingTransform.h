#pragma once
#include <Processors/ISimpleTransform.h>
#include <Core/SortDescription.h>

namespace DB
{

/** Sorts each block individually by the values of the specified columns.
  * At the moment, not very optimal algorithm is used.
  */
class PartialSortingTransform : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    /// When count_rows is false, getNumReadRows() will always return 0.
    PartialSortingTransform(
        const Block & header_,
        SortDescription & description_,
        UInt64 limit_ = 0,
        bool do_count_rows_ = true);

    String getName() const override { return "PartialSortingTransform"; }

    /// Total num rows passed to transform.
    UInt64 getNumReadRows() const { return read_rows; }

protected:
    void transform(Chunk & chunk) override;

private:
    SortDescription description;
    UInt64 limit;
    UInt64 read_rows = 0;

    /// Do we need calculate read_rows value?
    /// Used to skip total row when count rows_before_limit_at_least.
    bool do_count_rows;
};

}
