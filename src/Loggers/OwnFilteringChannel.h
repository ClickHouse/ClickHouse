#pragma once
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "OwnPatternFormatter.h"


namespace DB
{

// Filters the logs based on regular expressions. Should be processed after formatting channel to read entire formatted text
class OwnFilteringChannel : public Poco::Channel
{
public:
    explicit OwnFilteringChannel(Poco::AutoPtr<Poco::Channel> pChannel_, Poco::AutoPtr<OwnPatternFormatter> pf,
        std::string positive_pattern_, std::string negative_pattern_)
    : positive_pattern(positive_pattern_), negative_pattern(negative_pattern_), pChannel(pChannel_), pFormatter(pf)
    {
    }

    // Only log if pass both positive and negative regexp checks.
    // Checks the regexps on the formatted text (without color), but then passes the raw text
    // to the split channel to handle formatting for individual channels (e.g apply color)
    void log(const Poco::Message & msg) override;

    // Sets the regex patterns to use for filtering. Specifying an empty string pattern "" indicates no filtering
    void setRegexpPatterns(std::string positive_pattern_, std::string negative_pattern_)
    {
        positive_pattern = positive_pattern_;
        negative_pattern = negative_pattern_;
    }

    void open() override
    {
        if (pChannel)
            pChannel->open();
    }

    void close() override
    {
        if (pChannel)
            pChannel->close();
    }

    void setProperty(const std::string& name, const std::string& value) override
    {
        if (pChannel)
            pChannel->setProperty(name, value);
    }

private:
    bool regexpFilteredOut(std::string text) const;

    std::string positive_pattern;
    std::string negative_pattern;
    Poco::AutoPtr<Poco::Channel> pChannel;
    Poco::AutoPtr<OwnPatternFormatter> pFormatter;
};

}
