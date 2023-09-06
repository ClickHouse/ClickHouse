#include <Common/LoggingFormatStringHelpers.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}

std::unordered_map<UInt64, std::pair<time_t, size_t>> LogFrequencyLimiterIml::logged_messages;
time_t LogFrequencyLimiterIml::last_cleanup = 0;
std::mutex LogFrequencyLimiterIml::mutex;

void LogFrequencyLimiterIml::log(Poco::Message & message)
{
    std::string_view pattern = message.getFormatString();
    if (pattern.empty())
    {
        /// Do not filter messages without a format string
        if (auto * channel = logger->getChannel())
            channel->log(message);
        return;
    }

    SipHash hash;
    hash.update(logger->name());
    /// Format strings are compile-time constants, so they are uniquely identified by pointer and size
    hash.update(pattern.data());
    hash.update(pattern.size());

    time_t now = time(nullptr);
    size_t skipped_similar_messages = 0;
    bool need_cleanup;
    bool need_log;

    {
        std::lock_guard lock(mutex);
        need_cleanup = last_cleanup + 300 <= now;
        auto & info = logged_messages[hash.get64()];
        need_log = info.first + min_interval_s <= now;
        if (need_log)
        {
            skipped_similar_messages = info.second;
            info.first = now;
            info.second = 0;
        }
        else
        {
            ++info.second;
        }
    }

    /// We don't need all threads to do cleanup, just randomize
    if (need_cleanup && thread_local_rng() % 100 == 0)
        cleanup();

    /// The message it too frequent, skip it for now
    /// NOTE It's not optimal because we format the message first and only then check if we need to actually write it, see LOG_IMPL macro
    if (!need_log)
        return;

    if (skipped_similar_messages)
        message.appendText(fmt::format(" (skipped {} similar messages)", skipped_similar_messages));

    if (auto * channel = logger->getChannel())
        channel->log(message);
}

void LogFrequencyLimiterIml::cleanup(time_t too_old_threshold_s)
{
    time_t now = time(nullptr);
    time_t old = now - too_old_threshold_s;
    std::lock_guard lock(mutex);
    std::erase_if(logged_messages, [old](const auto & elem) { return elem.second.first < old; });
    last_cleanup = now;
}
