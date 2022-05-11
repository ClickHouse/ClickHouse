#include "OwnJSONPatternFormatter.h"

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/terminalColors.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>

OwnJSONPatternFormatter::OwnJSONPatternFormatter() : Poco::PatternFormatter("")
{
}


void OwnJSONPatternFormatter::formatExtendedJSON(const DB::ExtendedLogMessage & msg_ext, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    DB::FormatSettings settings;
    String key_name;

    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);

    key_name = "date_time";
    writeJSONString(StringRef(key_name), wb, settings);
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

    key_name = "thread_name";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(StringRef(msg.getThread()), wb, settings);

    DB::writeChar(',', wb);

    key_name = "thread_id";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg_ext.thread_id, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    key_name = "level";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    int priority = static_cast<int>(msg.getPriority());
    writeJSONString(StringRef(getPriorityName(priority)), wb, settings);

    DB::writeChar(',', wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.

    key_name = "query_id";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(msg_ext.query_id, wb, settings);

    DB::writeChar(',', wb);

    key_name = "logger_name";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);

    writeJSONString(StringRef(msg.getSource()), wb, settings);

    DB::writeChar(',', wb);

    key_name = "message";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    String msg_text = msg.getText();
    writeJSONString(StringRef(msg_text), wb, settings);

    DB::writeChar(',', wb);

    key_name = "source_file";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    const char * source_file = msg.getSourceFile();
    if (source_file != nullptr)
    {
        writeJSONString(StringRef(source_file), wb, settings);
    }

    else
    {
        writeJSONString(StringRef(""), wb, settings);
    }

    DB::writeChar(',', wb);

    key_name = "source_line";
    writeJSONString(StringRef(key_name), wb, settings);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg.getSourceLine(), wb);
    DB::writeChar('\"', wb);

    DB::writeChar('}', wb);
}

void OwnJSONPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtendedJSON(DB::ExtendedLogMessage::getFrom(msg), text);
}
