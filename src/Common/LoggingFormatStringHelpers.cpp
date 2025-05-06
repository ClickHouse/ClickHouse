#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}

std::unordered_map<UInt64, std::pair<time_t, size_t>> LogFrequencyLimiterImpl::logged_messages;
time_t LogFrequencyLimiterImpl::last_cleanup = 0;
std::mutex LogFrequencyLimiterImpl::mutex;

void LogFrequencyLimiterImpl::log(Poco::Message & message)
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
    hash.update(reinterpret_cast<uintptr_t>(pattern.data()));
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

void LogFrequencyLimiterImpl::cleanup(time_t too_old_threshold_s)
{
    time_t now = time(nullptr);
    time_t old = now - too_old_threshold_s;
    std::lock_guard lock(mutex);
    std::erase_if(logged_messages, [old](const auto & elem) { return elem.second.first < old; });
    last_cleanup = now;
}


std::mutex LogSeriesLimiter::mutex;
time_t LogSeriesLimiter::last_cleanup = 0;

LogSeriesLimiter::LogSeriesLimiter(LoggerPtr logger_, size_t allowed_count_, time_t interval_s_)
    : logger(std::move(logger_))
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

    if (last_cleanup == 0)
        last_cleanup = now;

    auto & series_records = getSeriesRecords();

    static const time_t cleanup_delay_s = 600;
    if (last_cleanup + cleanup_delay_s >= now)
    {
        time_t old = now - cleanup_delay_s;
        std::erase_if(series_records, [old](const auto & elem) { return get<0>(elem.second) < old; });
        last_cleanup = now;
    }

    auto register_as_first = [&] () TSA_REQUIRES(mutex)
    {
        assert(allowed_count_ > 0);
        accepted = true;
        series_records[name_hash] = std::make_tuple(now, 1, 1);
    };

    if (!series_records.contains(name_hash))
    {
        register_as_first();
        return;
    }

    auto & [last_time, accepted_count, total_count] = series_records[name_hash];
    if (last_time + interval_s_ <= now)
    {
        debug_message = fmt::format(
            " (LogSeriesLimiter: on interval from {} to {} accepted series {} / {} for the logger {})",
            DateLUT::instance().timeToString(last_time),
            DateLUT::instance().timeToString(now),
            accepted_count,
            total_count,
            logger->name());

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
