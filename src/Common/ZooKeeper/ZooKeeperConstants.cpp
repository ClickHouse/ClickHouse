#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <unordered_set>

namespace Coordination
{

static const std::unordered_set<int32_t> VALID_OPERATIONS =
{
    static_cast<int32_t>(OpNum::Close),
    static_cast<int32_t>(OpNum::Error),
    static_cast<int32_t>(OpNum::Create),
    static_cast<int32_t>(OpNum::Remove),
    static_cast<int32_t>(OpNum::Exists),
    static_cast<int32_t>(OpNum::Get),
    static_cast<int32_t>(OpNum::Set),
    static_cast<int32_t>(OpNum::SimpleList),
    static_cast<int32_t>(OpNum::Sync),
    static_cast<int32_t>(OpNum::Heartbeat),
    static_cast<int32_t>(OpNum::List),
    static_cast<int32_t>(OpNum::Check),
    static_cast<int32_t>(OpNum::Reconfig),
    static_cast<int32_t>(OpNum::Multi),
    static_cast<int32_t>(OpNum::MultiRead),
    static_cast<int32_t>(OpNum::CreateIfNotExists),
    static_cast<int32_t>(OpNum::Auth),
    static_cast<int32_t>(OpNum::SessionID),
    static_cast<int32_t>(OpNum::SetACL),
    static_cast<int32_t>(OpNum::GetACL),
    static_cast<int32_t>(OpNum::FilteredList),
    static_cast<int32_t>(OpNum::CheckNotExists),
    static_cast<int32_t>(OpNum::RemoveRecursive),
    static_cast<int32_t>(OpNum::CheckStat),
};

OpNum getOpNum(int32_t raw_op_num)
{
    if (!VALID_OPERATIONS.contains(raw_op_num))
        throw Exception(Error::ZUNIMPLEMENTED, "Operation {} is unknown", raw_op_num);
    return static_cast<OpNum>(raw_op_num);
}

const char * toOperationTypeMetricLabel(OpNum op_num)
{
    switch (op_num)
    {
        case OpNum::Exists:
        case OpNum::Get:
        case OpNum::GetACL:
        case OpNum::SimpleList:
        case OpNum::List:
        case OpNum::FilteredList:
        case OpNum::Check:
        case OpNum::CheckNotExists:
        case OpNum::CheckStat:
            return "readonly";

        case OpNum::Multi:
            return "multi";

        case OpNum::MultiRead:
            return "multi-read";

        case OpNum::Create:
        case OpNum::Remove:
        case OpNum::Set:
        case OpNum::SetACL:
        case OpNum::Sync:
        case OpNum::CreateIfNotExists:
        case OpNum::RemoveRecursive:
        case OpNum::Reconfig:
            return "write";

        case OpNum::Close:
        case OpNum::Error:
        case OpNum::Heartbeat:
        case OpNum::Auth:
        case OpNum::SessionID:
            return "session";
    }
    UNREACHABLE();
}

}
