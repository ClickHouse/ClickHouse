#pragma once
#include <IO/WriteBuffer.h>
#include <Core/Block.h>


namespace DB
{

/// Prints internal server logs or profile events with colored output (if requested).
/// NOTE: IRowOutputFormat does not suite well for this case
class InternalTextLogs
{
public:
    InternalTextLogs(WriteBuffer & buf_out, bool color_) : wb(buf_out), color(color_) {}

    /// Print internal server logs
    ///
    /// Input blocks have to have the same structure as SystemLogsQueue::getSampleBlock():
    /// - event_time
    /// - event_time_microseconds
    /// - host_name
    /// - query_id
    /// - thread_id
    /// - priority
    /// - source
    /// - text
    void writeLogs(const Block & block);
    /// Print profile events.
    ///
    /// Block:
    /// - host_name
    /// - current_time
    /// - thread_id
    /// - type
    /// - name
    /// - value
    ///
    /// See also TCPHandler::sendProfileEvents() for block columns.
    void writeProfileEvents(const Block & block);

    void flush()
    {
        wb.next();
    }

private:
    WriteBuffer & wb;
    bool color;
};

}
