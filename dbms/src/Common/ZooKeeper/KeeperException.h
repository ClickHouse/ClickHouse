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
inline bool isHardwareError(int32_t zk_return_code)
{
    return zk_return_code == ZooKeeperImpl::ZooKeeper::ZINVALIDSTATE
        || zk_return_code == ZooKeeperImpl::ZooKeeper::ZSESSIONEXPIRED
        || zk_return_code == ZooKeeperImpl::ZooKeeper::ZSESSIONMOVED
        || zk_return_code == ZooKeeperImpl::ZooKeeper::ZCONNECTIONLOSS
        || zk_return_code == ZooKeeperImpl::ZooKeeper::ZOPERATIONTIMEOUT;
}

/// Valid errors sent from server
inline bool isUserError(int32_t zk_return_code)
{
    return zk_return_code == ZooKeeperImpl::ZooKeeper::ZNONODE
           || zk_return_code == ZooKeeperImpl::ZooKeeper::ZBADVERSION
           || zk_return_code == ZooKeeperImpl::ZooKeeper::ZNOCHILDRENFOREPHEMERALS
           || zk_return_code == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS
           || zk_return_code == ZooKeeperImpl::ZooKeeper::ZNOTEMPTY;
}


class KeeperException : public DB::Exception
{
private:
    /// delegate constructor, used to minimize repetition; last parameter used for overload resolution
    KeeperException(const std::string & msg, const int32_t code, int)
        : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code) { incrementEventCounter(); }

public:
    KeeperException(const std::string & msg, const int32_t code)
        : KeeperException(msg + " (" + ZooKeeperImpl::ZooKeeper::errorMessage(code) + ")", code, 0) {}
    explicit KeeperException(const int32_t code) : KeeperException(ZooKeeperImpl::ZooKeeper::errorMessage(code), code, 0) {}
    KeeperException(const int32_t code, const std::string & path)
        : KeeperException(std::string{ZooKeeperImpl::ZooKeeper::errorMessage(code)} + ", path: " + path, code, 0) {}

    KeeperException(const KeeperException & exc) : DB::Exception(exc), code(exc.code) { incrementEventCounter(); }

    const char * name() const throw() override { return "zkutil::KeeperException"; }
    const char * className() const throw() override { return "zkutil::KeeperException"; }
    KeeperException * clone() const override { return new KeeperException(*this); }

    /// Any error related with network or master election
    /// In case of these errors you should reinitialize ZooKeeper session.
    bool isHardwareError() const
    {
        return zkutil::isHardwareError(code);
    }

    const int32_t code;

private:
    static void incrementEventCounter()
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperExceptions);
    }

};


class KeeperMultiException : public KeeperException
{
public:
    Requests requests;
    Responses responses;
    size_t failed_op_index = 0;

    std::string getPathForFirstFailedOp() const;

    /// If it is user error throws KeeperMultiException else throws ordinary KeeperException
    /// If it is ZOK does nothing
    static void check(int32_t code, const Requests & requests, const Responses & responses);

    KeeperMultiException(int32_t code, const Requests & requests, const Responses & responses);

private:
    size_t getFailedOpIndex(int32_t code, const Responses & responses) const;
};

};
