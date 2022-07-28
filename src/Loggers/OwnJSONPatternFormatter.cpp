#include "OwnJSONPatternFormatter.h"

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/terminalColors.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>

OwnJSONPatternFormatter::OwnJSONPatternFormatter() : OwnPatternFormatter("")
{
}


void OwnJSONPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB::WriteBufferFromString wb(text);

    DB::FormatSettings settings;

    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);

    writeJSONString("date_time", wb, settings);
    DB::writeChar(':', wb);

    DB::writeChar('\"', wb);
    /// Change delimiters in date for compatibility with old logs.
    writeDateTimeUnixTimestamp(msg_ext.time_seconds, 0, wb);
    DB::writeChar('.', wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 100000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 10000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 1000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 100) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 10) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 1) % 10), wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    writeJSONString("thread_name", wb, settings);
    DB::writeChar(':', wb);

    writeJSONString(msg.getThread(), wb, settings);

    DB::writeChar(',', wb);

    writeJSONString("thread_id", wb, settings);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg_ext.thread_id, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    writeJSONString("level", wb, settings);
    DB::writeChar(':', wb);
    int priority = static_cast<int>(msg.getPriority());
    writeJSONString(std::to_string(priority), wb, settings);
    DB::writeChar(',', wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.

    writeJSONString("query_id", wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(msg_ext.query_id, wb, settings);

    DB::writeChar(',', wb);

    writeJSONString("logger_name", wb, settings);
    DB::writeChar(':', wb);

    writeJSONString(msg.getSource(), wb, settings);
    DB::writeChar(',', wb);

    writeJSONString("message", wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(msg.getText(), wb, settings);
    DB::writeChar(',', wb);

    writeJSONString("source_file", wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(msg.getSourceFile(), wb, settings);
    DB::writeChar(',', wb);

    writeJSONString("source_line", wb, settings);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg.getSourceLine(), wb);
    DB::writeChar('\"', wb);

    DB::writeChar('}', wb);
}

void OwnJSONPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtended(DB::ExtendedLogMessage::getFrom(msg), text);
}
