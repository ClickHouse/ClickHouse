#include "SystemLogsRowOutputStream.h"
#include <Core/Block.h>
#include <Core/SystemLogsQueue.h>
#include <Common/typeid_cast.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

Block SystemLogsRowOutputStream::getHeader() const
{
    return SystemLogsQueue::getSampleBlock();
}


void SystemLogsRowOutputStream::write(const Block & block)
{
    for (size_t i = 0; i < block.rows(); ++i)
        write(block, i);
}


void SystemLogsRowOutputStream::write(const Block & block, size_t row_num)
{
    UInt32 event_time = typeid_cast<const ColumnUInt32 &>(*block.getByName("event_time").column).getData()[row_num];
    writeDateTimeText<'.', ':'>(event_time, wb);

    UInt32 microseconds = typeid_cast<const ColumnUInt32 &>(*block.getByName("event_time_microseconds").column).getData()[row_num];
    writeChar('.', wb);
    writeChar('0' + ((microseconds / 100000) % 10), wb);
    writeChar('0' + ((microseconds / 10000) % 10), wb);
    writeChar('0' + ((microseconds / 1000) % 10), wb);
    writeChar('0' + ((microseconds / 100) % 10), wb);
    writeChar('0' + ((microseconds / 10) % 10), wb);
    writeChar('0' + ((microseconds / 1) % 10), wb);

    writeCString(" [ ", wb);
    UInt32 thread_number = typeid_cast<const ColumnUInt32 &>(*block.getByName("thread_number").column).getData()[row_num];
    writeIntText(thread_number, wb);
    writeCString(" ] <", wb);

    Int8 priority = typeid_cast<const ColumnInt8 &>(*block.getByName("priority").column).getData()[row_num];
    writeString(SystemLogsQueue::getProrityName(priority), wb);
    writeCString("> ", wb);

    writeString(typeid_cast<const ColumnString &>(*block.getByName("source").column).getDataAt(row_num), wb);
    writeCString(": ", wb);
    writeString(typeid_cast<const ColumnString &>(*block.getByName("text").column).getDataAt(row_num), wb);

    writeChar('\n', wb);
}

BlockOutputStreamPtr SystemLogsRowOutputStream::create(WriteBuffer & buf_out)
{
    return std::make_shared<SystemLogsRowOutputStream>(buf_out);
}

}
