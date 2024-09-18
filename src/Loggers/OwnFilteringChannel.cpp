#include "OwnFilteringChannel.h"
#include <Poco/RegularExpression.h>


namespace DB
{

void OwnFilteringChannel::log(const Poco::Message & msg)
{
    std::string formatted_text;

    // Apply formatting to the text
    if (pFormatter)
    {
        pFormatter->formatExtended(ExtendedLogMessage::getFrom(msg), formatted_text);
    }
    else
    {
        formatted_text = msg.getText();
    }
    if (!regexpFilteredOut(formatted_text))
        pChannel->log(msg);
}

bool OwnFilteringChannel::regexpFilteredOut(std::string text) const
{
    if (!positive_pattern.empty())
    {
        Poco::RegularExpression positive_regexp(positive_pattern);
        if (!positive_regexp.match(text))
        {
            return true;
        }
    }

    if (!negative_pattern.empty())
    {
        Poco::RegularExpression negative_regexp(negative_pattern);
        if (negative_regexp.match(text))
        {
            return true;
        }
    }
    return false;
}

}
