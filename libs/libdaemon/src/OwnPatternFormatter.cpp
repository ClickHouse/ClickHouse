#include <daemon/OwnPatternFormatter.h>

#include <functional>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <optional>
#include <sys/time.h>
#include <Poco/Ext/ThreadNumber.h>
#include <daemon/BaseDaemon.h>


void OwnPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    /// For syslog: tag must be before message and first whitespace.
    if (options & ADD_LAYER_TAG && daemon)
    {
        auto layer = daemon->getLayer();
        if (layer)
        {
            writeCString("layer[", wb);
            DB::writeIntText(*layer, wb);
            writeCString("]: ", wb);
        }
    }

    /// Output time with microsecond resolution.
    timeval tv;
    if (0 != gettimeofday(&tv, nullptr))
        DB::throwFromErrno("Cannot gettimeofday");

    /// Change delimiters in date for compatibility with old logs.
    DB::writeDateTimeText<'.', ':'>(tv.tv_sec, wb);

    DB::writeChar('.', wb);
    DB::writeChar('0' + ((tv.tv_usec / 100000) % 10), wb);
    DB::writeChar('0' + ((tv.tv_usec / 10000) % 10), wb);
    DB::writeChar('0' + ((tv.tv_usec / 1000) % 10), wb);
    DB::writeChar('0' + ((tv.tv_usec / 100) % 10), wb);
    DB::writeChar('0' + ((tv.tv_usec / 10) % 10), wb);
    DB::writeChar('0' + ((tv.tv_usec / 1) % 10), wb);

    writeCString(" [ ", wb);
    DB::writeIntText(Poco::ThreadNumber::get(), wb);
    writeCString(" ] <", wb);
    DB::writeString(getPriorityName(static_cast<int>(msg.getPriority())), wb);
    writeCString("> ", wb);
    DB::writeString(msg.getSource(), wb);
    writeCString(": ", wb);
    DB::writeString(msg.getText(), wb);
}
