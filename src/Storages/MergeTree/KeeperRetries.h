#pragma once
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <base/sleep.h>

namespace DB{

namespace ErrorCodes
{
    extern const int OK;
}

struct RetriesInfo
{
    RetriesInfo() = default;
    RetriesInfo(UInt64 max_retries_, UInt64 initial_backoff_ms_, UInt64 max_backoff_ms_)
        : max_retries(max_retries_), curr_backoff_ms(initial_backoff_ms_), max_backoff_ms(max_backoff_ms_)
    {
    }

    UInt64 max_retries = 0;
    UInt64 curr_backoff_ms = 1;
    UInt64 max_backoff_ms = 0;
    UInt64 retry_count = 0;
};

class RetriesControl
{
public:
    explicit RetriesControl(RetriesInfo& retries_info_)
        : retries_info(retries_info_)
    {
    }

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

    bool canTry()
    {
        ++iteration_count;
        /// first iteration is ordinary execution, no further checks needed
        if (0 == iteration_count - 1)
            return true;

        if (unconditional_retry)
        {
            unconditional_retry = false;
            return true;
        }

        /// iteration succeeded -> no need to retry
        if (iteration_succeeded)
            return false;

        /// the flag will set to false in case of error
        iteration_succeeded = true;

        if (retries_info.retry_count >= retries_info.max_retries)
        {
            throwIfError();
            return false;
        }

        /// retries
        ++retries_info.retry_count;
        sleepForMilliseconds(retries_info.curr_backoff_ms);
        retries_info.curr_backoff_ms = std::min(retries_info.curr_backoff_ms * 2, retries_info.max_backoff_ms);

        return true;
    }

    void throwIfError() const
    {
        if (user_error.code != ErrorCodes::OK)
            throw Exception(user_error.code, user_error.message);

        if (keeper_error.code != KeeperError::Code::ZOK)
            throw zkutil::KeeperException(keeper_error.code, keeper_error.message);
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

    void setKeeperError(KeeperError::Code code, std::string message)
    {
        iteration_succeeded = false;
        keeper_error.code = code;
        keeper_error.message = std::move(message);
        user_error = UserError{};
    }

    bool call(auto && f)
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

    const auto & getUserError() const { return user_error; }

    const auto & getKeeperError() const { return keeper_error; }

    void requestUnconditionalRetry() { unconditional_retry = true; }

private:
    RetriesInfo& retries_info;
    UInt64 iteration_count = 0;
    UserError user_error;
    KeeperError keeper_error;
    bool unconditional_retry = false;
    bool iteration_succeeded = true;
};

}
