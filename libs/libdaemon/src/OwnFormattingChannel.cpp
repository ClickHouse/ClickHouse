#include <daemon/OwnFormattingChannel.h>
#include <daemon/OwnPatternFormatter.h>


namespace DB
{

void OwnFormattingChannel::logExtended(const ExtendedLogMessage & msg)
{
    if (pChannel && priority >= msg.base.getPriority())
    {
        if (pFormatter)
        {
            std::string text;
            pFormatter->formatExtended(msg, text);
            pChannel->log(Poco::Message(msg.base, text));
        }
        else
        {
            pChannel->log(msg.base);
        }
    }
}

void OwnFormattingChannel::log(const Poco::Message & msg)
{
    logExtended(ExtendedLogMessage::getFrom(msg));
}

OwnFormattingChannel::~OwnFormattingChannel() = default;


}
