#pragma once
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Message.h>
#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Loggers/OwnPatternFormatter.h>
#include <shared_mutex>


namespace DB
{

// Filters the logs based on regular expressions. Should be processed after formatting channel to read entire formatted text
class OwnFilteringChannel : public Poco::Channel
{
public:
    explicit OwnFilteringChannel(Poco::AutoPtr<Poco::Channel> pChannel_, Poco::AutoPtr<OwnPatternFormatter> pf,
        const std::string & positive_pattern_, const std::string & negative_pattern_, const std::string & name_)
    : logger_name(name_), positive_pattern(positive_pattern_), negative_pattern(negative_pattern_), pChannel(pChannel_), pFormatter(pf)
    {
    }

    explicit OwnFilteringChannel(Poco::AutoPtr<OwnFilteringChannel> other, const std::string & positive_pattern_, const std::string & negative_pattern_, const std::string & name_)
    : logger_name(name_), positive_pattern(positive_pattern_), negative_pattern(negative_pattern_), pChannel(other->pChannel), pFormatter(other->pFormatter)
    {
    }

    // Only log if pass both positive and negative regexp checks.
    // Checks the regexps on the formatted text (without color), but then passes the raw text
    // to the split channel to handle formatting for individual channels (e.g apply color)
    void log(const Poco::Message & msg) override;

    // Sets the regex patterns to use for filtering. Specifying an empty string pattern "" indicates no filtering
    void setRegexpPatterns(const std::string & new_pos_pattern, const std::string & new_neg_pattern);

    std::string getAssignedLoggerName() const
    {
        return logger_name;
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

    void setProperty(const std::string & name, const std::string & value) override
    {
        if (pChannel)
            pChannel->setProperty(name, value);
    }

    std::string getProperty(const std::string & name) const override
    {
        if (pChannel)
            return pChannel->getProperty(name);
        return "";
    }

private:
    bool regexpFilteredOut(const Poco::Message & msg);

    // Create copy safely, so we don't have to worry about race conditions from reading and writing at the same time
    std::pair<std::string, std::string> safeGetPatterns();

    const std::string logger_name;
    std::string positive_pattern;
    std::string negative_pattern;
    Poco::AutoPtr<Poco::Channel> pChannel;
    Poco::AutoPtr<OwnPatternFormatter> pFormatter;
    std::shared_mutex pattern_mutex;
};

// Creates filter channel only if needed or updates if it already exists
void createOrUpdateFilterChannel(Poco::Logger & logger, const std::string & pos_pattern, const std::string & neg_pattern, Poco::AutoPtr<OwnPatternFormatter> pf, const std::string & name = "");

}
