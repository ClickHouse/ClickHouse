#include <daemon/ExtendedLogChannel.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <common/getThreadNumber.h>
#include <sys/time.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_GETTIMEOFDAY;
}

ExtendedLogMessage ExtendedLogMessage::getFrom(const Poco::Message & base)
{
    ExtendedLogMessage msg_ext(base);

    ::timeval tv;
    if (0 != gettimeofday(&tv, nullptr))
        DB::throwFromErrno("Cannot gettimeofday", ErrorCodes::CANNOT_GETTIMEOFDAY);

    msg_ext.time_seconds = static_cast<UInt32>(tv.tv_sec);
    msg_ext.time_microseconds = static_cast<UInt32>(tv.tv_usec);

    if (current_thread)
        msg_ext.query_id = CurrentThread::getQueryId();

    msg_ext.thread_number = getThreadNumber();

    return msg_ext;
}

}
