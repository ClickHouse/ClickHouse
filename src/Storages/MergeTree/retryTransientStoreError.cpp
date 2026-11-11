#include <Storages/MergeTree/retryTransientStoreError.h>

#include <Interpreters/TransactionManager.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <system_error>

namespace DB
{

namespace
{

constexpr UInt64 RETRY_TIMEOUT_SECONDS = 60;
constexpr UInt64 RETRY_BACKOFF_MS = 100;
constexpr UInt64 RETRY_MAX_BACKOFF_MS = 2000;

/// The errnos a local write fails with when the disk, not the data, is the problem. The
/// `ErrnoException` branch of `isRetryableException` lists only the read side, and its
/// `filesystem_error` branch has no `EIO`.
bool isTransientWriteErrno(int err)
{
    return err == ENOSPC || err == EDQUOT || err == EROFS || err == EIO || err == EBUSY || err == ETIMEDOUT || err == EAGAIN;
}

/// `isRetryableException` adjusted for a write: plus the write-side errnos, however they are
/// reported, minus the memory limits. A memory limit is the calling query's, not the storage's,
/// and would be retried under the locks the query holds; the `noexcept` callers block memory
/// exceptions anyway.
bool isTransientStoreError(std::exception_ptr exception)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const ErrnoException & e)
    {
        if (isNotEnoughMemoryErrorCode(e.code()))
            return false;
        return isTransientWriteErrno(e.getErrno()) || isRetryableException(exception);
    }
    catch (const Exception & e)
    {
        if (isNotEnoughMemoryErrorCode(e.code()))
            return false;
        return isRetryableException(exception);
    }
    catch (const std::filesystem::filesystem_error & e)
    {
        /// `DiskLocal::replaceFile` is `fs::rename`, and reports a failure this way.
        const auto & category = e.code().category();
        if ((category == std::generic_category() || category == std::system_category()) && isTransientWriteErrno(e.code().value()))
            return true;
        return isRetryableException(exception);
    }
    catch (...)
    {
        /// Ok: not swallowed, `isRetryableException` knows the other types (Azure, Poco).
        return isRetryableException(exception);
    }
}

}

thread_local const TransientStoreRetryScope * current_retry_scope = nullptr;

TransientStoreRetryScope::TransientStoreRetryScope()
    : outer(current_retry_scope)
{
    current_retry_scope = this;
}

TransientStoreRetryScope::~TransientStoreRetryScope()
{
    current_retry_scope = outer;
}

const TransientStoreRetryScope * TransientStoreRetryScope::current()
{
    return current_retry_scope;
}

void retryTransientStoreError(LoggerPtr log, std::string_view what, const std::function<void()> & store)
{
    const TransientStoreRetryScope * scope = TransientStoreRetryScope::current();
    UInt64 backoff_ms = RETRY_BACKOFF_MS;
    size_t attempts = 0;
    while (true)
    {
        ++attempts;
        try
        {
            store();
            if (attempts > 1)
                LOG_INFO(log, "Stored transaction metadata for {} after {} attempts", what, attempts);
            return;
        }
        catch (...)
        {
            if (!scope || !isTransientStoreError(std::current_exception()))
                throw;

            /// `instanceIfAny`: this path is taken by non-transactional writes too, and constructing
            /// the manager here would turn a disk error into a Keeper one.
            const TransactionManager * transaction_manager = TransactionManager::instanceIfAny();
            const bool give_up = scope->elapsedSeconds() >= RETRY_TIMEOUT_SECONDS
                || (transaction_manager && transaction_manager->isShuttingDown());
            if (give_up)
            {
                LOG_ERROR(log, "Cannot store transaction metadata for {} after {} attempts in {:.1f} s, giving up: {}",
                    what, attempts, scope->elapsedSeconds(), getCurrentExceptionMessage(false));
                throw;
            }

            if (attempts == 1)
                LOG_WARNING(log, "Cannot store transaction metadata for {}, will retry: {}", what, getCurrentExceptionMessage(false));
            else
                LOG_DEBUG(log, "Cannot store transaction metadata for {}, attempt {}: {}", what, attempts, getCurrentExceptionMessage(false));
        }

        sleepForMilliseconds(backoff_ms);
        backoff_ms = std::min(backoff_ms * 2, RETRY_MAX_BACKOFF_MS);
    }
}

}
