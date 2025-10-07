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

const char * toMetricLabel(OpNum op_num)
{
    switch (op_num)
    {
        case OpNum::Close: return "close";
        case OpNum::Error: return "error";
        case OpNum::Create: return "create";
        case OpNum::Remove: return "remove";
        case OpNum::Exists: return "exists";
        case OpNum::Get: return "get";
        case OpNum::Set: return "set";
        case OpNum::GetACL: return "get_acl";
        case OpNum::SetACL: return "set_acl";
        case OpNum::SimpleList: return "simple_list";
        case OpNum::Sync: return "sync";
        case OpNum::Heartbeat: return "heartbeat";
        case OpNum::List: return "list";
        case OpNum::Check: return "check";
        case OpNum::Multi: return "multi";
        case OpNum::Reconfig: return "reconfig";
        case OpNum::MultiRead: return "multi_read";
        case OpNum::Auth: return "auth";
        case OpNum::FilteredList: return "filtered_list";
        case OpNum::CheckNotExists: return "check_not_exists";
        case OpNum::CreateIfNotExists: return "create_if_not_exists";
        case OpNum::RemoveRecursive: return "remove_recursive";
        case OpNum::CheckStat: return "check_stat";
        case OpNum::SessionID: return "session_id";
    }
    UNREACHABLE();
}

}
