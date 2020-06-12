#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Common/SharedBlockRowRef.h>
#include <Core/SortDescription.h>


namespace DB
{


/** Implements the LIMIT relational operation.
  */
class LimitBlockInputStream : public IBlockInputStream
{
public:
    /** If always_read_till_end = false (by default), then after reading enough data,
      *  returns an empty block, and this causes the query to be canceled.
      * If always_read_till_end = true - reads all the data to the end, but ignores them. This is necessary in rare cases:
      *  when otherwise, due to the cancellation of the request, we would not have received the data for GROUP BY WITH TOTALS from the remote server.
      * If use_limit_as_total_rows_approx = true, then addTotalRowsApprox is called to use the limit in progress & stats
      * with_ties = true, when query has WITH TIES modifier. If so, description should be provided
      * description lets us know which row we should check for equality
      */
    LimitBlockInputStream(
            const BlockInputStreamPtr & input, UInt64 limit_, UInt64 offset_,
            bool always_read_till_end_ = false, bool use_limit_as_total_rows_approx = false,
            bool with_ties_ = false, const SortDescription & description_ = {});

    String getName() const override { return "Limit"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    UInt64 limit;
    UInt64 offset;
    UInt64 pos = 0;
    bool always_read_till_end;
    bool with_ties;
    const SortDescription description;
    SharedBlockRowRef ties_row_ref;
};

}
