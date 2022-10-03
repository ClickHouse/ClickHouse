#pragma once
#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int OK;
}

struct KeeperRetriesInfo
{
    KeeperRetriesInfo() = default;
    KeeperRetriesInfo(std::string name_, Poco::Logger * logger_, UInt64 max_retries_, UInt64 initial_backoff_ms_, UInt64 max_backoff_ms_)
        : name(std::move(name_))
        , logger(logger_)
        , max_retries(max_retries_)
        , curr_backoff_ms(initial_backoff_ms_)
        , max_backoff_ms(max_backoff_ms_)
    {
    }

    std::string name;
    Poco::Logger * logger = nullptr;
    UInt64 max_retries = 0;
    UInt64 curr_backoff_ms = 0;
    UInt64 max_backoff_ms = 0;
    UInt64 retry_count = 0;
};

class KeeperRetriesControl
{
public:
    KeeperRetriesControl(std::string name_, KeeperRetriesInfo & retries_info_) : name(std::move(name_)), retries_info(retries_info_) { }

    bool canTry()
    {
        ++iteration_count;
        /// first iteration is ordinary execution, no further checks needed
        if (0 == iteration_count)
            return true;

        if (unconditional_retry)
        {
            unconditional_retry = false;
            return true;
        }

        /// iteration succeeded -> no need to retry
        if (iteration_succeeded)
        {
            if (retries_info.logger)
                LOG_DEBUG(
                    retries_info.logger,
                    "{}/{}: succeeded after: iterations={} total_retries={}",
                    retries_info.name,
                    name,
                    iteration_count,
                    retries_info.retry_count);
            return false;
        }

        if (retries_info.retry_count >= retries_info.max_retries)
        {
            logLastError("retry limit is reached");
            throwIfError();
            return false;
        }

        /// retries
        logLastError("retry due to error");
        ++retries_info.retry_count;
        sleepForMilliseconds(retries_info.curr_backoff_ms);
        retries_info.curr_backoff_ms = std::min(retries_info.curr_backoff_ms * 2, retries_info.max_backoff_ms);

        /// reset the flag, will set to false in case of error
        iteration_succeeded = true;

        return true;
    }

    void retryLoop(auto && f)
    {
        while (canTry())
        {
            try
            {
                f();
            }
            catch (const zkutil::KeeperException & e)
            {
                if (!Coordination::isHardwareError(e.code))
                    throw;

                setKeeperError(e.code, e.message());
            }
        }
    }

    bool callAndCatchAll(auto && f)
    {
        try
        {
            f();
            return true;
        }
        catch (const zkutil::KeeperException & e)
        {
            setKeeperError(e.code, e.message());
        }
        catch (const Exception & e)
        {
            setUserError(e.code(), e.what());
        }
        return false;
    }

    void setUserError(int code, std::string message)
    {
        iteration_succeeded = false;
        user_error.code = code;
        user_error.message = std::move(message);
        keeper_error = KeeperError{};
    }

    template <typename... Args>
    void setUserError(int code, fmt::format_string<Args...> fmt, Args &&... args)
    {
        setUserError(code, fmt::format(fmt, std::forward<Args>(args)...));
    }

    void setKeeperError(Coordination::Error code, std::string message)
    {
        iteration_succeeded = false;
        keeper_error.code = code;
        keeper_error.message = std::move(message);
        user_error = UserError{};
    }

    void requestUnconditionalRetry() { unconditional_retry = true; }

private:
    struct KeeperError
    {
        using Code = Coordination::Error;
        Code code = Code::ZOK;
        std::string message;
    };

    struct UserError
    {
        int code = ErrorCodes::OK;
        std::string message;
    };

    void throwIfError() const
    {
        if (user_error.code != ErrorCodes::OK)
            throw Exception(user_error.code, user_error.message);

        if (keeper_error.code != KeeperError::Code::ZOK)
            throw zkutil::KeeperException(keeper_error.code, keeper_error.message);
    }

    void logLastError(std::string header)
    {
        if (user_error.code == ErrorCodes::OK)
        {
            if (retries_info.logger)
                LOG_DEBUG(
                    retries_info.logger,
                    "{}/{}: {}: retry_count={} timeout={}ms error={} message={}",
                    retries_info.name,
                    name,
                    header,
                    retries_info.retry_count,
                    retries_info.curr_backoff_ms,
                    keeper_error.code,
                    keeper_error.message);
        }
        else
        {
            if (retries_info.logger)
                LOG_DEBUG(
                    retries_info.logger,
                    "{}/{}: {}: retry_count={} timeout={}ms error={} message={}",
                    retries_info.name,
                    name,
                    header,
                    retries_info.retry_count,
                    retries_info.curr_backoff_ms,
                    user_error.code,
                    user_error.message);
        }
    }


    std::string name;
    KeeperRetriesInfo & retries_info;
    Int64 iteration_count = -1;
    UserError user_error;
    KeeperError keeper_error;
    bool unconditional_retry = false;
    bool iteration_succeeded = true;
};

}
