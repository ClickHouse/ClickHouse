#include <shared_mutex>
#include <Loggers/OwnFilteringChannel.h>
#include <Poco/RegularExpression.h>


namespace DB
{

void OwnFilteringChannel::log(const Poco::Message & msg)
{
    if (regexpFilteredOut(msg))
        return;

    pChannel->log(msg);
}

bool OwnFilteringChannel::regexpFilteredOut(const Poco::Message & msg)
{
    std::string formatted_text;
    auto [pos_pattern, neg_pattern] = safeGetPatterns();

    // Skip checks if both patterns are empty
    if (!pos_pattern.empty() || !neg_pattern.empty())
    {
        // Apply formatting to the text
        if (pFormatter)
        {
            pFormatter->formatExtended(ExtendedLogMessage::getFrom(msg), formatted_text);
        }
        else
        {
            formatted_text = msg.getText();
        }

        // Check for patterns in formatted text
        Poco::RegularExpression positive_regexp(pos_pattern);
        if (!pos_pattern.empty() && !positive_regexp.match(formatted_text))
        {
            return true;
        }

        Poco::RegularExpression negative_regexp(neg_pattern);
        if (!neg_pattern.empty() && negative_regexp.match(formatted_text))
        {
            return true;
        }
    }

    return false;
}

std::pair<std::string, std::string> OwnFilteringChannel::safeGetPatterns()
{
    std::shared_lock<std::shared_mutex> read_lock(pattern_mutex);
    return std::make_pair(positive_pattern, negative_pattern);
}

}
