#include "OwnJSONPatternFormatter.h"

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/terminalColors.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>

OwnJSONPatternFormatter::OwnJSONPatternFormatter(Poco::Util::AbstractConfiguration & config_) : OwnPatternFormatter(), config(config_)
{
    if (config.has("logger.formatting.names.date_time"))
        this->date_time = config.getString("logger.formatting.names.date_time", "");

    if (config.has("logger.formatting.names.thread_name"))
        this->thread_name = config.getString("logger.formatting.names.thread_name", "");

    if (config.has("logger.formatting.names.thread_id"))
        this->thread_id = config.getString("logger.formatting.names.thread_id", "");

    if (config.has("logger.formatting.names.level"))
        this->level = config.getString("logger.formatting.names.level", "");

    if (config.has("logger.formatting.names.query_id"))
        this->query_id = config.getString("logger.formatting.names.query_id", "");

    if (config.has("logger.formatting.names.logger_name"))
        this->logger_name = config.getString("logger.formatting.names.logger_name", "");

    if (config.has("logger.formatting.names.message"))
        this->message = config.getString("logger.formatting.names.message", "");

    if (config.has("logger.formatting.names.source_file"))
        this->source_file_ = config.getString("logger.formatting.names.source_file", "");

    if (config.has("logger.formatting.names.source_line"))
        this->source_line = config.getString("logger.formatting.names.source_line", "");

    if (this->date_time.empty() && this->thread_name.empty() && this->thread_id.empty() && this->level.empty() && this->query_id.empty()
        && this->logger_name.empty() && this->message.empty() && this->source_file_.empty() && this->source_line.empty())
    {
        date_time = "date_time";
        thread_name = "thread_name";
        thread_id = "thread_id";
        level = "level";
        query_id = "query_id";
        logger_name = "logger_name";
        message = "message";
        source_file_ = "source_file";
        source_line = "source_line";
    }
}

void OwnJSONPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text) const
{
    DB::WriteBufferFromString wb(text);

    DB::FormatSettings settings;
    bool print_comma = false;

    const Poco::Message & msg = msg_ext.base;
    DB::writeChar('{', wb);

    if (!this->date_time.empty())
    {
        writeJSONString(this->date_time, wb, settings);
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

    if (!this->thread_name.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->thread_name, wb, settings);
        DB::writeChar(':', wb);

        writeJSONString(msg.getThread(), wb, settings);
    }

    if (!this->thread_id.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->thread_id, wb, settings);
        DB::writeChar(':', wb);
        DB::writeChar('\"', wb);
        DB::writeIntText(msg_ext.thread_id, wb);
        DB::writeChar('\"', wb);
    }

    if (!this->level.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->level, wb, settings);
        DB::writeChar(':', wb);
        int priority = static_cast<int>(msg.getPriority());
        writeJSONString(std::to_string(priority), wb, settings);
    }

    if (!this->query_id.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        /// We write query_id even in case when it is empty (no query context)
        /// just to be convenient for various log parsers.

        writeJSONString(this->query_id, wb, settings);
        DB::writeChar(':', wb);
        writeJSONString(msg_ext.query_id, wb, settings);
    }

    if (!this->logger_name.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->logger_name, wb, settings);
        DB::writeChar(':', wb);

        writeJSONString(msg.getSource(), wb, settings);
    }

    if (!this->message.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->message, wb, settings);
        DB::writeChar(':', wb);
        writeJSONString(msg.getText(), wb, settings);
    }

    if (!this->source_file_.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);
        else
            print_comma = true;

        writeJSONString(this->source_file_, wb, settings);
        DB::writeChar(':', wb);
        const char * source_file = msg.getSourceFile();
        if (source_file != nullptr)
            writeJSONString(source_file, wb, settings);
        else
            writeJSONString("", wb, settings);
    }

    if (!this->source_line.empty())
    {
        if (print_comma)
            DB::writeChar(',', wb);

        writeJSONString(this->source_line, wb, settings);
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
