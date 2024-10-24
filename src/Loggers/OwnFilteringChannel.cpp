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

void OwnFilteringChannel::setRegexpPatterns(const std::string & new_pos_pattern, const std::string & new_neg_pattern)
{
    auto [old_pos_pattern, old_neg_pattern] = safeGetPatterns();
    if (old_pos_pattern != new_pos_pattern || old_neg_pattern != new_neg_pattern)
    {
        std::unique_lock<std::shared_mutex> write_lock(pattern_mutex);
        positive_pattern = new_pos_pattern;
        negative_pattern = new_neg_pattern;
    }
}

std::pair<std::string, std::string> OwnFilteringChannel::safeGetPatterns()
{
    std::shared_lock<std::shared_mutex> read_lock(pattern_mutex);
    return std::make_pair(positive_pattern, negative_pattern);
}

void createOrUpdateFilterChannel(Poco::Logger & logger, const std::string & pos_pattern, const std::string & neg_pattern, Poco::AutoPtr<OwnPatternFormatter> pf, const std::string & name)
{
    Poco::AutoPtr<Poco::Channel> src_channel(logger.getChannel(), true /*shared*/);
    Poco::AutoPtr<DB::OwnFilteringChannel> filter_channel(dynamic_cast<DB::OwnFilteringChannel*>(src_channel.get()), true);

    // If this logger doesn't have it's own unique filter channel
    if (!filter_channel)
    {
        // Skip if regexp feature has never been used yet
        if (pos_pattern.empty() && neg_pattern.empty())
            return;

        Poco::AutoPtr<DB::OwnFilteringChannel> new_filter_channel = new DB::OwnFilteringChannel(src_channel, pf, pos_pattern, neg_pattern, name);
        logger.setChannel(new_filter_channel);
    }
    // If logger has filter channel, but not it's own unique one (e.g copied from another by default), create copy
    else if (filter_channel->getAssignedLoggerName() != name)
    {
        Poco::AutoPtr<DB::OwnFilteringChannel> new_filter_channel = new DB::OwnFilteringChannel(filter_channel, pos_pattern, neg_pattern, name);
        logger.setChannel(new_filter_channel);
    }
    else
    {
        filter_channel->setRegexpPatterns(pos_pattern, neg_pattern);
    }
}

}
