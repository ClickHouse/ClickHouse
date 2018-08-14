#pragma once

#include "Types.h"


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


using KeeperException = ZooKeeperImpl::Exception;


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
    static size_t getFailedOpIndex(int32_t code, const Responses & responses);
};

}
