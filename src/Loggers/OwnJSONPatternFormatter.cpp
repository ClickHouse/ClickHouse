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


void OwnJSONPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    DB::FormatSettings settings;
    char key_name[] = "a placeholder for key names in structured logging";
    char empty_string[] = "";

    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);

    strcpy(key_name, "date_time");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
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

    strcpy(key_name, "thread_name");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);

    const char * thread_name = msg.getThread().c_str();
    if (thread_name != nullptr)
        writeJSONString(thread_name, thread_name + strlen(thread_name), wb, settings);
    else
        writeJSONString(empty_string, empty_string + strlen(empty_string), wb, settings);

    DB::writeChar(',', wb);

    strcpy(key_name, "thread_id");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);
    DB::writeChar('\"', wb);
    DB::writeIntText(msg_ext.thread_id, wb);
    DB::writeChar('\"', wb);

    DB::writeChar(',', wb);

    strcpy(key_name, "level");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);
    int priority_int = static_cast<int>(msg.getPriority());
    String priority_str = std::to_string(priority_int);
    const char * priority = priority_str.c_str();
    if (priority != nullptr)
        writeJSONString(priority, priority + strlen(priority), wb, settings);
    else
        writeJSONString(empty_string, empty_string + strlen(empty_string), wb, settings);

    DB::writeChar(',', wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.

    strcpy(key_name, "query_id");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);
    writeJSONString(msg_ext.query_id, wb, settings);

    DB::writeChar(',', wb);

    strcpy(key_name, "logger_name");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);

    const char * logger_name = msg.getSource().c_str();
    if (logger_name != nullptr)
        writeJSONString(logger_name, logger_name + strlen(logger_name), wb, settings);
    else
        writeJSONString(empty_string, empty_string + strlen(empty_string), wb, settings);

    DB::writeChar(',', wb);

    strcpy(key_name, "message");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);
    const char * msg_text = msg.getText().c_str();
    if (msg_text != nullptr)
        writeJSONString(msg_text, msg_text + strlen(msg_text), wb, settings);
    else
        writeJSONString(empty_string, empty_string + strlen(empty_string), wb, settings);

    DB::writeChar(',', wb);

    strcpy(key_name, "source_file");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
    DB::writeChar(':', wb);
    const char * source_file = msg.getSourceFile();
    if (source_file != nullptr)
        writeJSONString(source_file, source_file + strlen(source_file), wb, settings);
    else
        writeJSONString(empty_string, empty_string + strlen(empty_string), wb, settings);

    DB::writeChar(',', wb);

    strcpy(key_name, "source_line");
    writeJSONString(key_name, key_name + strlen(key_name), wb, settings);
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
