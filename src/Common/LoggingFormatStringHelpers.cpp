#include <Common/DateLUT.h>
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


std::unordered_map<UInt64, std::tuple<size_t, time_t>> LogSeriesLimiter::series_settings;
std::unordered_map<UInt64, std::tuple<time_t, size_t, size_t>> LogSeriesLimiter::series_loggers;
std::mutex LogSeriesLimiter::mutex;

LogSeriesLimiter::LogSeriesLimiter(Poco::Logger * logger_, size_t allowed_count_, time_t interval_s_)
    : logger(logger_)
{
    if (allowed_count_ == 0)
    {
        accepted = false;
        return;
    }

    if (interval_s_ == 0)
    {
        accepted = true;
        return;
    }

    time_t now = time(nullptr);
    UInt128 name_hash = sipHash128(logger->name().c_str(), logger->name().size());

    std::lock_guard lock(mutex);

    if (series_settings.contains(name_hash))
    {
        auto & settings = series_settings[name_hash];
        auto & [allowed_count, interval_s] = settings;
        chassert(allowed_count_ == allowed_count);
        chassert(interval_s_ == interval_s);
    }
    else
    {
        series_settings[name_hash] = std::make_tuple(allowed_count_, interval_s_);
    }

    auto register_as_first = [&] () TSA_REQUIRES(mutex)
    {
        assert(allowed_count_ > 0);
        accepted = true;
        series_loggers[name_hash] = std::make_tuple(now, 1, 1);
    };


    if (!series_loggers.contains(name_hash))
    {
        register_as_first();
        return;
    }

    auto & [last_time, accepted_count, total_count] = series_loggers[name_hash];
    if (last_time + interval_s_ <= now)
    {
        debug_message = fmt::format(
            " (LogSeriesLimiter: on interval from {} to {} accepted series {} / {} for the logger {} : {})",
            DateLUT::instance().timeToString(last_time),
            DateLUT::instance().timeToString(now),
            accepted_count,
            total_count,
            logger->name(),
            double(name_hash));

        register_as_first();
        return;
    }

    if (accepted_count < allowed_count_)
    {
        accepted = true;
        ++accepted_count;
    }
    ++total_count;
}

void LogSeriesLimiter::log(Poco::Message & message)
{
    std::string_view pattern = message.getFormatString();
    if (pattern.empty())
    {
        /// Do not filter messages without a format string
        if (auto * channel = logger->getChannel())
            channel->log(message);
        return;
    }

    if (!accepted)
        return;

    if (!debug_message.empty())
    {
        message.appendText(debug_message);
        debug_message.clear();
    }

    if (auto * channel = logger->getChannel())
        channel->log(message);
}
