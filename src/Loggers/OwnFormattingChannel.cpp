#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


OwnFormattingChannel::OwnFormattingChannel(Poco::AutoPtr<OwnPatternFormatter> pFormatter_, Poco::AutoPtr<Poco::Channel> pChannel_)
    : pFormatter(pFormatter_)
    , pChannel(pChannel_)
{
    if (!pFormatter_)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Formatter cannot be null");
    if (!pChannel_)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Channel cannot be null");
}

void OwnFormattingChannel::setChannel(Poco::AutoPtr<Poco::Channel> pChannel_)
{
    if (!pChannel_)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Channel cannot be null");
    pChannel = pChannel_;
}

void OwnFormattingChannel::logExtended(const ExtendedLogMessage & msg)
{
    std::string text;
    pFormatter->formatExtended(msg, text);
    pChannel->log(Poco::Message(*msg.base, text));
}

void OwnFormattingChannel::log(const Poco::Message &)
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "OwnFormattingChannel called with Poco::Message instead of ExtendedLogMessage");
}

void OwnFormattingChannel::log(Poco::Message && msg)
{
    std::string text;
    ExtendedLogMessage extended(msg);
    pFormatter->formatExtended(extended, text);
    msg.setText(text);
    pChannel->log(std::move(msg));
}

OwnFormattingChannel::~OwnFormattingChannel() = default;

}
