#pragma once
#include <Common/Exception.h>
#include "Types.h"
#include <Common/ProfileEvents.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int KEEPER_EXCEPTION;
    }
}

namespace ProfileEvents
{
    extern const Event ZooKeeperExceptions;
}


namespace zkutil
{


/// You should reinitialize ZooKeeper session in case of these errors
inline bool isUnrecoverableErrorCode(int32_t zk_return_code)
{
    return zk_return_code == ZINVALIDSTATE || zk_return_code == ZSESSIONEXPIRED || zk_return_code == ZSESSIONMOVED;
}

/// Errors related with temporary network problems
inline bool isTemporaryErrorCode(int32_t zk_return_code)
{
    return zk_return_code == ZCONNECTIONLOSS || zk_return_code == ZOPERATIONTIMEOUT;
}

/// Any error related with network or master election
/// In case of these errors you should retry the query or reinitialize ZooKeeper session (see isUnrecoverable())
inline bool isHardwareErrorCode(int32_t zk_return_code)
{
    return isUnrecoverableErrorCode(zk_return_code) || isTemporaryErrorCode(zk_return_code);
}

/// Valid errors sent from server
inline bool isUserError(int32_t zk_return_code)
{
    return zk_return_code == ZNONODE
           || zk_return_code == ZBADVERSION
           || zk_return_code == ZNOCHILDRENFOREPHEMERALS
           || zk_return_code == ZNODEEXISTS
           || zk_return_code == ZNOTEMPTY;
}


class KeeperException : public DB::Exception
{
private:
    /// delegate constructor, used to minimize repetition; last parameter used for overload resolution
    KeeperException(const std::string & msg, const int32_t code, int)
        : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code) { incrementEventCounter(); }

public:
    explicit KeeperException(const std::string & msg) : KeeperException(msg, ZOK, 0) {}
    KeeperException(const std::string & msg, const int32_t code)
        : KeeperException(msg + " (" + zerror(code) + ")", code, 0) {}
    explicit KeeperException(const int32_t code) : KeeperException(zerror(code), code, 0) {}
    KeeperException(const int32_t code, const std::string & path)
        : KeeperException(std::string{zerror(code)} + ", path: " + path, code, 0) {}

    KeeperException(const KeeperException & exc) : DB::Exception(exc), code(exc.code) { incrementEventCounter(); }

    const char * name() const throw() override { return "zkutil::KeeperException"; }
    const char * className() const throw() override { return "zkutil::KeeperException"; }
    KeeperException * clone() const override { return new KeeperException(*this); }

    /// You should reinitialize ZooKeeper session in case of these errors
    bool isUnrecoverable() const
    {
        return isUnrecoverableErrorCode(code);
    }

    /// Errors related with temporary network problems
    bool isTemporaryError() const
    {
        return isTemporaryErrorCode(code);
    }

    /// Any error related with network or master election
    /// In case of these errors you should retry the query or reinitialize ZooKeeper session (see isUnrecoverable())
    bool isHardwareError() const
    {
        return isHardwareErrorCode(code);
    }

    const int32_t code;

private:
    static void incrementEventCounter()
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperExceptions);
    }

};

};
