#include "OwnPatternFormatter.h"

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/terminalColors.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>

OwnPatternFormatter::OwnPatternFormatter(bool color_) : Poco::PatternFormatter(""), color(color_)
{
}

void OwnPatternFormatter::formatExtendedJSON(const DB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB::WriteBufferFromString wb(text);
    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);
    DB::writeChar('\"', wb);
    DB::writeString("date_time", wb); // key
    DB::writeChar('\"', wb);
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

    DB::writeChar('\"', wb);
    DB::writeString("thread_name", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeString(msg.getThread(), wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    DB::writeString("thread_id", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg_ext.thread_id, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    DB::writeString("level", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    int priority = static_cast<int>(msg.getPriority());
    DB::writeString(getPriorityName(priority), wb);
    DB::writeChar('\"', wb);
    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.
    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    writeString("query_id", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    writeEscapedString(msg_ext.query_id, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    writeString("logger_name", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);

    DB::writeChar('\"', wb);

    writeEscapedString(msg.getSource(), wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    writeString("message", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    String msg_text = msg.getText();
    writeEscapedString(msg_text, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    writeString("source_file", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    const char * source_file = msg.getSourceFile();
    if (source_file != nullptr)
    {
        DB::writeString(source_file, wb);
    }

    else
    {
        DB::writeString("", wb);
    }
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    DB::writeChar('\"', wb);
    writeString("source_line", wb);
    DB::writeChar('\"', wb);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg.getSourceLine(), wb);
    DB::writeChar('\"', wb);
    DB::writeChar('}', wb);
}

void OwnPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB::WriteBufferFromString wb(text);

    const Poco::Message & msg = msg_ext.base;

    /// Change delimiters in date for compatibility with old logs.
    DB::writeDateTimeText<'.', ':'>(msg_ext.time_seconds, wb);

    DB::writeChar('.', wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 100000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 10000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 1000) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 100) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 10) % 10), wb);
    DB::writeChar('0' + ((msg_ext.time_microseconds / 1) % 10), wb);

    writeCString(" [ ", wb);
    if (color)
        writeString(setColor(intHash64(msg_ext.thread_id)), wb);
    DB::writeIntText(msg_ext.thread_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(" ] ", wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.
    writeCString("{", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg_ext.query_id)), wb);
    DB::writeString(msg_ext.query_id, wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("} ", wb);

    writeCString("<", wb);
    int priority = static_cast<int>(msg.getPriority());
    if (color)
        writeCString(setColorForLogPriority(priority), wb);
    DB::writeString(getPriorityName(priority), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString("> ", wb);
    if (color)
        writeString(setColor(std::hash<std::string>()(msg.getSource())), wb);
    DB::writeString(msg.getSource(), wb);
    if (color)
        writeCString(resetColor(), wb);
    writeCString(": ", wb);
    DB::writeString(msg.getText(), wb);
}

void OwnPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtended(DB::ExtendedLogMessage::getFrom(msg), text);
}
