#include <Client/InternalTextLogs.h>
#include <Core/Block.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/typeid_cast.h>
#include <Common/HashTable/Hash.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <IO/WriteHelpers.h>
#include <base/terminalColors.h>


namespace DB
{

void InternalTextLogs::writeLogs(const Block & block)
{
    const auto & array_event_time = typeid_cast<const ColumnUInt32 &>(*block.getByName("event_time").column).getData();
    const auto & array_microseconds = typeid_cast<const ColumnUInt32 &>(*block.getByName("event_time_microseconds").column).getData();

    const auto & column_host_name = typeid_cast<const ColumnString &>(*block.getByName("host_name").column);
    const auto & column_query_id = typeid_cast<const ColumnString &>(*block.getByName("query_id").column);

    const auto & array_thread_id = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & array_priority = typeid_cast<const ColumnInt8 &>(*block.getByName("priority").column).getData();
    const auto & column_source = typeid_cast<const ColumnString &>(*block.getByName("source").column);
    const auto & column_text = typeid_cast<const ColumnString &>(*block.getByName("text").column);

    for (size_t row_num = 0; row_num < block.rows(); ++row_num)
    {
        auto host_name = column_host_name.getDataAt(row_num);
        if (host_name.size)
        {
            writeCString("[", wb);
            if (color)
                writeString(setColor(StringRefHash()(host_name)), wb);
            writeString(host_name, wb);
            if (color)
                writeCString(resetColor(), wb);
            writeCString("] ", wb);
        }

        auto event_time = array_event_time[row_num];
        writeDateTimeText<'.', ':'>(event_time, wb);

        auto microseconds = array_microseconds[row_num];
        writeChar('.', wb);
        writeChar('0' + ((microseconds / 100000) % 10), wb);
        writeChar('0' + ((microseconds / 10000) % 10), wb);
        writeChar('0' + ((microseconds / 1000) % 10), wb);
        writeChar('0' + ((microseconds / 100) % 10), wb);
        writeChar('0' + ((microseconds / 10) % 10), wb);
        writeChar('0' + ((microseconds / 1) % 10), wb);

        UInt64 thread_id = array_thread_id[row_num];
        writeCString(" [ ", wb);
        if (color)
            writeString(setColor(intHash64(thread_id)), wb);
        writeIntText(thread_id, wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString(" ]", wb);

        auto query_id = column_query_id.getDataAt(row_num);
        if (query_id.size)
        {
            writeCString(" {", wb);
            if (color)
                writeString(setColor(StringRefHash()(query_id)), wb);
            writeString(query_id, wb);
            if (color)
                writeCString(resetColor(), wb);
            writeCString("}", wb);
        }

        Int8 priority = array_priority[row_num];
        writeCString(" <", wb);
        if (color)
            writeCString(setColorForLogPriority(priority), wb);
        writeString(InternalTextLogsQueue::getPriorityName(priority), wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString("> ", wb);

        auto source = column_source.getDataAt(row_num);
        if (color)
            writeString(setColor(StringRefHash()(source)), wb);
        DB::writeString(source, wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString(": ", wb);

        auto text = column_text.getDataAt(row_num);
        writeString(text, wb);

        writeChar('\n', wb);
    }
}

void InternalTextLogs::writeProfileEvents(const Block & block)
{
    const auto & column_host_name = typeid_cast<const ColumnString &>(*block.getByName("host_name").column);
    const auto & array_current_time = typeid_cast<const ColumnUInt32 &>(*block.getByName("current_time").column).getData();
    const auto & array_thread_id = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & array_type = typeid_cast<const ColumnInt8 &>(*block.getByName("type").column).getData();
    const auto & column_name = typeid_cast<const ColumnString &>(*block.getByName("name").column);
    const auto & array_value = typeid_cast<const ColumnInt64 &>(*block.getByName("value").column).getData();

    for (size_t row_num = 0; row_num < block.rows(); ++row_num)
    {
        /// host_name
        auto host_name = column_host_name.getDataAt(row_num);
        if (host_name.size)
        {
            writeCString("[", wb);
            if (color)
                writeString(setColor(StringRefHash()(host_name)), wb);
            writeString(host_name, wb);
            if (color)
                writeCString(resetColor(), wb);
            writeCString("] ", wb);
        }

        /// current_time
        auto current_time = array_current_time[row_num];
        writeDateTimeText<'.', ':'>(current_time, wb);

        /// thread_id
        UInt64 thread_id = array_thread_id[row_num];
        writeCString(" [ ", wb);
        if (color)
            writeString(setColor(intHash64(thread_id)), wb);
        writeIntText(thread_id, wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString(" ] ", wb);

        /// name
        auto name = column_name.getDataAt(row_num);
        if (color)
            writeString(setColor(StringRefHash()(name)), wb);
        DB::writeString(name, wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString(": ", wb);

        /// value
        Int64 value = array_value[row_num];
        writeIntText(value, wb);

        //// type
        Int8 type = array_type[row_num];
        writeCString(" (", wb);
        if (color)
            writeString(setColor(intHash64(type)), wb);
        writeString(toString(ProfileEvents::TypeEnum->castToName(type)), wb);
        if (color)
            writeCString(resetColor(), wb);
        writeCString(")", wb);

        writeChar('\n', wb);
    }
}

}
