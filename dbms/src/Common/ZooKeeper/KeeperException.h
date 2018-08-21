#pragma once

#include "Types.h"


namespace zkutil
{


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
