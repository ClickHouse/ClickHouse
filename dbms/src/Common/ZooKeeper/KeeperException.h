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
    static void check(int32_t code, const Coordination::Requests & requests, const Coordination::Responses & responses);

    KeeperMultiException(int32_t code, const Coordination::Requests & requests, const Coordination::Responses & responses);

private:
    static size_t getFailedOpIndex(int32_t code, const Coordination::Responses & responses);
};

}
