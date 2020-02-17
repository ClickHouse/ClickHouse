#pragma once

#include <Columns/ColumnsNumber.h>
#include <DataStreams/IBlockInputStream.h>


namespace DB
{
/** Add timestamp column for processing time process in
 *  WINDOW VIEW
 */
class AddingTimestampBlockInputStream : public IBlockInputStream
{
public:
    AddingTimestampBlockInputStream(const BlockInputStreamPtr & input_, UInt32 timestamp_) : input(input_), timestamp(timestamp_)
    {
        cached_header = input->getHeader();
        cached_header.insert({ColumnUInt32::create(1, 1), std::make_shared<DataTypeDateTime>(), "____timestamp"});
    }

    String getName() const override { return "AddingTimestamp"; }

    Block getHeader() const override { return cached_header.cloneEmpty(); }

protected:
    Block readImpl() override
    {
        Block res = input->read();
        if (res)
            res.insert({ColumnUInt32::create(res.rows(), timestamp), std::make_shared<DataTypeDateTime>(), "____timestamp"});
        return res;
    }

private:
    BlockInputStreamPtr input;
    Block cached_header;
    UInt32 timestamp;
};
}
