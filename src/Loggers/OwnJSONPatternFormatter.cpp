#include <Loggers/OwnJSONPatternFormatter.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/terminalColors.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>


OwnJSONPatternFormatter::OwnJSONPatternFormatter(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (config.has(config_prefix + ".names.date_time"))
        date_time = config.getString(config_prefix + ".names.date_time", "");

    if (config.has(config_prefix + ".names.date_time_utc"))
        date_time_utc= config.getString(config_prefix + ".names.date_time_utc", "");

    if (config.has(config_prefix + ".names.thread_name"))
        thread_name = config.getString(config_prefix + ".names.thread_name", "");

    if (config.has(config_prefix + ".names.thread_id"))
        thread_id = config.getString(config_prefix + ".names.thread_id", "");

    if (config.has(config_prefix + ".names.level"))
        level = config.getString(config_prefix + ".names.level", "");

    if (config.has(config_prefix + ".names.query_id"))
        query_id = config.getString(config_prefix + ".names.query_id", "");

    if (config.has(config_prefix + ".names.logger_name"))
        logger_name = config.getString(config_prefix + ".names.logger_name", "");

    if (config.has(config_prefix + ".names.message"))
        message = config.getString(config_prefix + ".names.message", "");

    if (config.has(config_prefix + ".names.source_file"))
        source_file = config.getString(config_prefix + ".names.source_file", "");

    if (config.has(config_prefix + ".names.source_line"))
        source_line = config.getString(config_prefix + ".names.source_line", "");

    if (date_time.empty() && thread_name.empty() && thread_id.empty() && level.empty() && query_id.empty()
        && logger_name.empty() && message.empty() && source_file.empty() && source_line.empty())
    {
        date_time = "date_time";
        date_time_utc = "date_time_utc";
        thread_name = "thread_name";
        thread_id = "thread_id";
        level = "level";
        query_id = "query_id";
        logger_name = "logger_name";
        message = "message";
        source_file = "source_file";
        source_line = "source_line";
    }
}

void OwnJSONPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB::WriteBufferFromString wb(text);

    DB::FormatSettings settings;
    bool print_comma = false;

    const Poco::Message & msg = *msg_ext.base;
    DB::writeChar('{', wb);

    if (!date_time_utc.empty())
    {
        writeJSONString(date_time_utc, wb, settings);
        DB::writeChar(':', wb);

        DB::writeChar('\"', wb);
        static const DateLUTImpl & utc_time_zone = DateLUT::instance("UTC");
        writeDateTimeTextISO(msg_ext.time_seconds, 0, wb, utc_time_zone);

        DB::writeChar('\"', wb);
        print_comma = true;
    }

    if (!date_time.empty())
    {
        if (print_comma) DB::writeChar(',', wb);
        writeJSONString(date_time, wb, settings);
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


    if (!thread_name.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(thread_name, wb, settings);
        DB::writeChar(':', wb);

        writeJSONString(msg.getThread(), wb, settings);
    }

    if (!thread_id.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(thread_id, wb, settings);
        DB::writeChar(':', wb);
        DB::writeChar('\"', wb);
        DB::writeIntText(msg_ext.thread_id, wb);
        DB::writeChar('\"', wb);
    }

    if (!level.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(level, wb, settings);
        DB::writeChar(':', wb);
        int priority = static_cast<int>(msg.getPriority());
        writeJSONString(getPriorityName(priority), wb, settings);
    }

    if (!query_id.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        /// We write query_id even in case when it is empty (no query context)
        /// just to be convenient for various log parsers.

        writeJSONString(query_id, wb, settings);
        DB::writeChar(':', wb);
        writeJSONString(msg_ext.query_id, wb, settings);
    }

    if (!logger_name.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(logger_name, wb, settings);
        DB::writeChar(':', wb);

        writeJSONString(msg.getSource(), wb, settings);
    }

    if (!message.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(message, wb, settings);
        DB::writeChar(':', wb);
        writeJSONString(msg.getText(), wb, settings);
    }

    if (!source_file.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(source_file, wb, settings);
        DB::writeChar(':', wb);
        writeJSONString(msg.getSourceFile(), wb, settings);
    }

    if (!source_line.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);

        writeJSONString(source_line, wb, settings);
        DB::writeChar(':', wb);
        DB::writeChar('\"', wb);
        DB::writeIntText(msg.getSourceLine(), wb);
        DB::writeChar('\"', wb);
    }
    DB::writeChar('}', wb);
}

void OwnJSONPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtended(DB::ExtendedLogMessage::getFrom(msg), text);
}
