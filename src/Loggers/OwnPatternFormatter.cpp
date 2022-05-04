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

void OwnPatternFormatter::formatExtendedJSON(const DB::ExtendedLogMessage & msg_ext, std::string & text)
{
    DB::WriteBufferFromString wb(text);
    DB::FormatSettings settings;
    bool print_comma = false;

    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);

    if (!DB::ExtendedLogMessage::log_keys.key_date_time.empty())
    {
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_date_time), wb, settings);

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
        print_comma = true;
    }

    if (!DB::ExtendedLogMessage::log_keys.key_thread_name.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_thread_name), wb, settings);

        DB::writeChar(':', wb);

        writeJSONString(StringRef(msg.getThread()), wb, settings);
    }

    if (!DB::ExtendedLogMessage::log_keys.key_thread_id.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }

        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_thread_id), wb, settings);

        DB::writeChar(':', wb);

        DB::writeChar('\"', wb);
        DB::writeIntText(msg_ext.thread_id, wb);
        DB::writeChar('\"', wb);
    }

    if (!DB::ExtendedLogMessage::log_keys.key_level.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_level), wb, settings);

        DB::writeChar(':', wb);

        int priority = static_cast<int>(msg.getPriority());
        writeJSONString(StringRef(getPriorityName(priority)), wb, settings);
    }

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.

    if (!DB::ExtendedLogMessage::log_keys.key_query_id.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_query_id), wb, settings);

        DB::writeChar(':', wb);

        writeJSONString(msg_ext.query_id, wb, settings);
    }

    if (!DB::ExtendedLogMessage::log_keys.key_logger_name.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_logger_name), wb, settings);

        DB::writeChar(':', wb);

        writeJSONString(StringRef(msg.getSource()), wb, settings);
    }

    if (!DB::ExtendedLogMessage::log_keys.key_message.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_message), wb, settings);

        DB::writeChar(':', wb);

        String msg_text = msg.getText();
        writeJSONString(StringRef(msg_text), wb, settings);
    }

    if (!DB::ExtendedLogMessage::log_keys.key_source_file.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        else
        {
            print_comma = true;
        }
        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_source_file), wb, settings);

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
    }

    if (!DB::ExtendedLogMessage::log_keys.key_source_line.empty())
    {
        if (print_comma)
        {
            DB::writeChar(',', wb);
        }

        writeJSONString(StringRef(DB::ExtendedLogMessage::log_keys.key_source_line), wb, settings);

        DB::writeChar(':', wb);

        DB::writeChar('\"', wb);
        DB::writeIntText(msg.getSourceLine(), wb);
        DB::writeChar('\"', wb);
    }

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
