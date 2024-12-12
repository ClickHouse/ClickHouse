#pragma once
#include <Interpreters/ProcessList.h>
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

struct ZooKeeperRetriesInfo
{
    ZooKeeperRetriesInfo(UInt64 max_retries_, UInt64 initial_backoff_ms_, UInt64 max_backoff_ms_)
        : max_retries(max_retries_), initial_backoff_ms(std::min(initial_backoff_ms_, max_backoff_ms_)), max_backoff_ms(max_backoff_ms_)
    {
    }

    UInt64 max_retries;
    UInt64 initial_backoff_ms;
    UInt64 max_backoff_ms;
};

class ZooKeeperRetriesControl
{
public:
    ZooKeeperRetriesControl(std::string name_, LoggerPtr logger_, ZooKeeperRetriesInfo retries_info_, QueryStatusPtr elem)
        : name(std::move(name_)), logger(logger_), retries_info(retries_info_), process_list_element(elem)
    {
    }

    ZooKeeperRetriesControl(const ZooKeeperRetriesControl & other)
        : name(other.name)
        , logger(other.logger)
        , retries_info(other.retries_info)
        , total_failures(other.total_failures)
        , process_list_element(other.process_list_element)
        , current_backoff_ms(other.current_backoff_ms)
    {
    }

    void retryLoop(auto && f)
    {
        retryLoop(f, []() {});
    }

    /// retryLoop() executes f() until it succeeds/max_retries is reached/non-retryable error is encountered
    ///
    /// the callable f() can provide feedback in terms of errors in two ways:
    /// 1. throw KeeperException exception:
    ///     in such case, retries are done only on hardware keeper errors
    ///     because non-hardware error codes are semantically not really errors, just a response
    /// 2. set an error code in the ZooKeeperRetriesControl object (setUserError/setKeeperError)
    ///     The idea is that if the caller has some semantics on top of non-hardware keeper errors,
    ///     then it can provide feedback to retries controller via user errors
    ///
    /// It is possible to use it multiple times (it will share nº of errors over the total amount of calls)
    /// Each retryLoop is independent and it will execute f at least once
    void retryLoop(auto && f, auto && iteration_cleanup)
    {
        current_iteration = 0;
        current_backoff_ms = retries_info.initial_backoff_ms;

        while (current_iteration == 0 || canTry())
        {
            /// reset the flag, it will be set to false in case of error
            iteration_succeeded = true;
            try
            {
                f();
                iteration_cleanup();
            }
            catch (const zkutil::KeeperException & e)
            {
                iteration_cleanup();

                if (!Coordination::isHardwareError(e.code))
                    throw;

                setKeeperError(std::current_exception(), e.code, e.message());
            }
            catch (...)
            {
                iteration_cleanup();
                throw;
            }
            current_iteration++;
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
            setKeeperError(std::current_exception(), e.code, e.message());
        }
        catch (const Exception & e)
        {
            setUserError(std::current_exception(), e.code(), e.what());
        }
        return false;
    }

    void setUserError(std::exception_ptr exception, int code, const std::string & message)
    {
        if (logger)
            LOG_TRACE(logger, "ZooKeeperRetriesControl: {}: setUserError: error={} message={}", name, code, message);

        if (iteration_succeeded)
            total_failures++;

        iteration_succeeded = false;
        user_error.code = code;
        user_error.message = message;
        user_error.exception = exception;
        keeper_error = KeeperError{};
    }

    void setUserError(const Exception & exception)
    {
        setUserError(std::make_exception_ptr(exception), exception.code(), exception.message());
    }

    void setKeeperError(std::exception_ptr exception, Coordination::Error code, std::string message)
    {
        if (logger)
            LOG_TRACE(logger, "ZooKeeperRetriesControl: {}: setKeeperError: error={} message={}", name, code, message);

        if (iteration_succeeded)
            total_failures++;

        iteration_succeeded = false;
        keeper_error.code = code;
        keeper_error.message = std::move(message);
        keeper_error.exception = exception;
        user_error = UserError{};
    }

    void setKeeperError(const zkutil::KeeperException & exception)
    {
        setKeeperError(std::make_exception_ptr(exception), exception.code, exception.message());
    }

    void stopRetries() { stop_retries = true; }

    bool isLastRetry() const { return total_failures >= retries_info.max_retries; }

    bool isRetry() const { return current_iteration > 0; }

    const std::string & getLastKeeperErrorMessage() const { return keeper_error.message; }

    /// action will be called only once and only after latest failed retry
    /// NOTE: this one will be called only in case when retries finishes with Keeper exception
    /// if it will be some other exception this function will not be called.
    void actionAfterLastFailedRetry(std::function<void()> f) { action_after_last_failed_retry = std::move(f); }

    const std::string & getName() const { return name; }

    LoggerPtr getLogger() const { return logger; }

private:
    struct KeeperError
    {
        using Code = Coordination::Error;
        Code code = Code::ZOK;
        std::string message;
        std::exception_ptr exception;
    };

    struct UserError
    {
        int code = ErrorCodes::OK;
        std::string message;
        std::exception_ptr exception;
    };

    bool canTry()
    {
        if (unconditional_retry)
        {
            unconditional_retry = false;
            return true;
        }

        if (iteration_succeeded)
        {
            if (logger && total_failures > 0)
                LOG_DEBUG(
                    logger,
                    "ZooKeeperRetriesControl: {}: succeeded after: Iterations={} Total keeper failures={}/{}",
                    name,
                    current_iteration,
                    total_failures,
                    retries_info.max_retries);
            return false;
        }

        if (stop_retries)
        {
            action_after_last_failed_retry();
            logLastError("stop retries on request");
            throwIfError();
            return false;
        }

        if (total_failures > retries_info.max_retries)
        {
            action_after_last_failed_retry();
            logLastError("retry limit is reached");
            throwIfError();
            return false;
        }

        if (process_list_element)
            process_list_element->checkTimeLimit();

        /// retries
        logLastError("will retry due to error");
        sleepForMilliseconds(current_backoff_ms);
        current_backoff_ms = std::min(current_backoff_ms * 2, retries_info.max_backoff_ms);

        return true;
    }

    void throwIfError() const
    {
        if (user_error.exception)
            std::rethrow_exception(user_error.exception);

        if (keeper_error.exception)
            std::rethrow_exception(keeper_error.exception);
    }

    void logLastError(const std::string_view & header)
    {
        if (!logger)
            return;
        if (user_error.code == ErrorCodes::OK)
        {
            LOG_DEBUG(
                logger,
                "ZooKeeperRetriesControl: {}: {}: retry_count={}/{} timeout={}ms error={} message={}",
                name,
                header,
                current_iteration,
                retries_info.max_retries,
                current_backoff_ms,
                keeper_error.code,
                keeper_error.message);
        }
        else
        {
            LOG_DEBUG(
                logger,
                "ZooKeeperRetriesControl: {}: {}: retry_count={}/{} timeout={}ms error={} message={}",
                name,
                header,
                current_iteration,
                retries_info.max_retries,
                current_backoff_ms,
                user_error.code,
                user_error.message);
        }
    }


    std::string name;
    LoggerPtr logger = nullptr;
    ZooKeeperRetriesInfo retries_info;
    UInt64 total_failures = 0;
    UserError user_error;
    KeeperError keeper_error;
    std::function<void()> action_after_last_failed_retry = []() {};
    bool iteration_succeeded = true;
    bool stop_retries = false;
    QueryStatusPtr process_list_element;

    UInt64 current_iteration = 0;
    UInt64 current_backoff_ms = 0;

public:
    /// This is used in SharedMergeTree
    bool unconditional_retry = false;
};

}
