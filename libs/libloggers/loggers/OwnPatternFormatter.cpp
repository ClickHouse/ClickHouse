#include "OwnPatternFormatter.h"

#include <functional>
#include <optional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <sys/time.h>
#include <Common/CurrentThread.h>
#include <common/getThreadNumber.h>
#include "Loggers.h"


OwnPatternFormatter::OwnPatternFormatter(const Loggers * loggers_, OwnPatternFormatter::Options options_)
    : Poco::PatternFormatter(""), loggers(loggers_), options(options_)
{
}


void OwnPatternFormatter::formatExtended(const DB::ExtendedLogMessage & msg_ext, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    const Poco::Message & msg = msg_ext.base;

    /// For syslog: tag must be before message and first whitespace.
    /// This code is only used in Yandex.Metrika and unneeded in ClickHouse.
    if ((options & ADD_LAYER_TAG) && loggers)
    {
        auto layer = loggers->getLayer();
        if (layer)
        {
            writeCString("layer[", wb);
            DB::writeIntText(*layer, wb);
            writeCString("]: ", wb);
        }
    }

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
    DB::writeIntText(msg_ext.thread_number, wb);
    writeCString(" ] ", wb);

    /// We write query_id even in case when it is empty (no query context)
    /// just to be convenient for various log parsers.
    writeCString("{", wb);
    DB::writeString(msg_ext.query_id, wb);
    writeCString("} ", wb);

    writeCString("<", wb);
    DB::writeString(getPriorityName(static_cast<int>(msg.getPriority())), wb);
    writeCString("> ", wb);
    DB::writeString(msg.getSource(), wb);
    writeCString(": ", wb);
    DB::writeString(msg.getText(), wb);
}

void OwnPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    formatExtended(DB::ExtendedLogMessage::getFrom(msg), text);
}
