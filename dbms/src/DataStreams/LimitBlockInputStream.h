#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Implements the LIMIT relational operation.
  */
class LimitBlockInputStream : public IProfilingBlockInputStream
{
public:
    /** If always_read_till_end = false (by default), then after reading enough data,
      *  returns an empty block, and this causes the query to be canceled.
      * If always_read_till_end = true - reads all the data to the end, but ignores them. This is necessary in rare cases:
      *  when otherwise, due to the cancellation of the request, we would not have received the data for GROUP BY WITH TOTALS from the remote server.
      */
    LimitBlockInputStream(BlockInputStreamPtr input_, size_t limit_, size_t offset_, bool always_read_till_end_ = false);

    String getName() const override { return "Limit"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Limit(" << children.back()->getID() << ", " << limit << ", " << offset << ")";
        return res.str();
    }

protected:
    Block readImpl() override;

private:
    size_t limit;
    size_t offset;
    size_t pos = 0;
    bool always_read_till_end;
};

}
