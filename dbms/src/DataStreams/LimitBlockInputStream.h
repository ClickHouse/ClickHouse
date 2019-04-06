#pragma once

#include <DataStreams/IBlockInputStream.h>


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
      */
    LimitBlockInputStream(const BlockInputStreamPtr & input, UInt64 limit_, UInt64 offset_, bool always_read_till_end_ = false, bool use_limit_as_total_rows_approx = false);

    String getName() const override { return "Limit"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    UInt64 limit;
    UInt64 offset;
    UInt64 pos = 0;
    bool always_read_till_end;
};

}
