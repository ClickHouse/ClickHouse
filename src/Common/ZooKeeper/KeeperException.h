#pragma once

#include <Common/ZooKeeper/Types.h>


namespace zkutil
{


using KeeperException = Coordination::Exception;


class KeeperMultiException : public KeeperException
{
public:
    Coordination::Requests requests;
    Coordination::Responses responses;
    size_t failed_op_index = 0;

    std::string getPathForFirstFailedOp() const;

    /// If it is user error throws KeeperMultiException else throws ordinary KeeperException
    /// If it is ZOK does nothing
    static void check(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);

    KeeperMultiException(Coordination::Error code, const Coordination::Requests & requests, const Coordination::Responses & responses);
};

size_t getFailedOpIndex(Coordination::Error code, const Coordination::Responses & responses);
}
