#pragma once

#include <atomic>
#include <mutex>
#include <memory>

#include <base/types.h>
#include <Common/Logger.h>
#include <Common/SharedLockGuard.h>
#include <Common/SharedMutex.h>

namespace DB
{

/** AtomicLogger allows to atomically change logger.
  * Standard library does not have atomic_shared_ptr, and we do not use std::atomic* operations,
  * because standard library implementation uses fixed table of mutexes, and it is better to avoid contention here.
  */
class AtomicLogger
{
public:
    explicit AtomicLogger(LoggerPtr logger_)
        : logger(std::move(logger_))
    {}

    explicit AtomicLogger(const std::string & log_name)
        : AtomicLogger(::getLogger(log_name))
    {}

    void store(LoggerPtr new_logger)
    {
        std::lock_guard lock(log_mutex);
        logger = std::move(new_logger);
    }

    void store(const std::string & new_log_name)
    {
        auto new_logger = ::getLogger(new_log_name);
        store(std::move(new_logger));
    }

    LoggerPtr load() const
    {
        DB::SharedLockGuard lock(log_mutex);
        return logger;
    }

    String loadName() const
    {
        DB::SharedLockGuard lock(log_mutex);
        return logger->name();
    }
private:
    mutable DB::SharedMutex log_mutex;
    LoggerPtr logger;
};

}
