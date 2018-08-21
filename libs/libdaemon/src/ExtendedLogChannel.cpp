#include <daemon/ExtendedLogChannel.h>
#include <Common/Exception.h>
#include <Common/CurrentThread.h>
#include <Poco/Ext/ThreadNumber.h>
#include <sys/time.h>


namespace DB
{

ExtendedLogMessage ExtendedLogMessage::getFrom(const Poco::Message & base)
{
    ExtendedLogMessage msg_ext(base);

    ::timeval tv;
    if (0 != gettimeofday(&tv, nullptr))
        DB::throwFromErrno("Cannot gettimeofday");

    msg_ext.time_seconds = static_cast<UInt32>(tv.tv_sec);
    msg_ext.time_microseconds = static_cast<UInt32>(tv.tv_usec);
    msg_ext.query_id = CurrentThread::getCurrentQueryID();
    msg_ext.thread_number = Poco::ThreadNumber::get();

    return msg_ext;
}

}
