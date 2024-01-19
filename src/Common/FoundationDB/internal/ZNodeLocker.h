#pragma once

#include <boost/core/noncopyable.hpp>
#include <foundationdb/fdb_c.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/internal/KeeperCommon.h>
#include <Common/FoundationDB/internal/ZNodeLayer.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include "Coroutine.h"
#include "FDBExtended.h"

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace DB::FoundationDB
{

/// Local Async RWLock
struct ZNodeLocker : boost::noncopyable
{
    struct Builder
    {
        Builder() { holder = fdb_rwlocks_create(); }
        ~Builder()
        {
            if (holder)
                fdb_rwlocks_free(holder);
        }

        void shared(const std::string & key)
        {
            if (!holder)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ZNodeLocker Builder is locked");
            fdb_rwlocks_shared(holder, key.data());
        }

        void exclusive(const std::string & key)
        {
            if (!holder)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ZNodeLocker Builder is locked");
            fdb_rwlocks_exclusive(holder, key.data());
        }

        Builder & create(const std::string & path)
        {
            exclusive(path);
            const auto parent_path = getParentPath(path).toString();
            if (!ZNodeLayer::isRootPath(parent_path))
                shared(parent_path);
            return *this;
        }

        Builder & remove(const std::string & path)
        {
            exclusive(path);
            const auto parent_path = getParentPath(path).toString();
            shared(parent_path);
            return *this;
        }

        Builder & set(const std::string & path)
        {
            exclusive(path);
            return *this;
        }

        Coroutine::Task<ZNodeLocker> lock()
        {
            try
            {
                co_await fdb_rwlocks_lock(holder);
            }
            catch (...)
            {
                fdb_rwlocks_free(holder);
                throw;
            }

            ZNodeLocker l(holder);
            holder = nullptr;
            co_return std::move(l);
        }

    private:
        FDBRWLockHodler * holder = nullptr;
    };

private:
    explicit ZNodeLocker(FDBRWLockHodler * h) : holder(h) { }
    FDBRWLockHodler * holder = nullptr;
    friend struct Builder;

public:
    ZNodeLocker() = default;
    ZNodeLocker(ZNodeLocker && other) noexcept { std::swap(holder, other.holder); }
    ZNodeLocker & operator=(ZNodeLocker && other) noexcept
    {
        std::swap(holder, other.holder);
        return *this;
    }
    ~ZNodeLocker()
    {
        if (holder)
            fdb_rwlocks_free(holder);
    }
};
}
