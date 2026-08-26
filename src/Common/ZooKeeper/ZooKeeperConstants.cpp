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
    static_cast<int32_t>(OpNum::Create2),
    static_cast<int32_t>(OpNum::Remove),
    static_cast<int32_t>(OpNum::TryRemove),
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
    static_cast<int32_t>(OpNum::FilteredListWithStatsAndData),
    static_cast<int32_t>(OpNum::CheckNotExists),
    static_cast<int32_t>(OpNum::RemoveRecursive),
    static_cast<int32_t>(OpNum::CheckStat),
    static_cast<int32_t>(OpNum::AddWatch),
    static_cast<int32_t>(OpNum::CheckWatch),
    static_cast<int32_t>(OpNum::RemoveWatch),
    static_cast<int32_t>(OpNum::SetWatch),
    static_cast<int32_t>(OpNum::SetWatch2),
};

OpNum getOpNum(int32_t raw_op_num)
{
    if (!VALID_OPERATIONS.contains(raw_op_num))
        throw Exception(Error::ZUNIMPLEMENTED, "Operation {} is unknown", raw_op_num);
    return static_cast<OpNum>(raw_op_num);
}

std::string_view opNumToString(OpNum op_num)
{
    switch (op_num)
    {
        case OpNum::Close: return "Close";
        case OpNum::Error: return "Error";
        case OpNum::Create: return "Create";
        case OpNum::Create2: return "Create2";
        case OpNum::Remove: return "Remove";
        case OpNum::Exists: return "Exists";
        case OpNum::Get: return "Get";
        case OpNum::Set: return "Set";
        case OpNum::GetACL: return "GetACL";
        case OpNum::SetACL: return "SetACL";
        case OpNum::SimpleList: return "SimpleList";
        case OpNum::Sync: return "Sync";
        case OpNum::Heartbeat: return "Heartbeat";
        case OpNum::List: return "List";
        case OpNum::Check: return "Check";
        case OpNum::Multi: return "Multi";
        case OpNum::Reconfig: return "Reconfig";
        case OpNum::MultiRead: return "MultiRead";
        case OpNum::Auth: return "Auth";
        case OpNum::FilteredList: return "FilteredList";
        case OpNum::CheckNotExists: return "CheckNotExists";
        case OpNum::CreateIfNotExists: return "CreateIfNotExists";
        case OpNum::RemoveRecursive: return "RemoveRecursive";
        case OpNum::SessionID: return "SessionID";
        case OpNum::CheckStat: return "CheckStat";
        case OpNum::AddWatch: return "AddWatch";
        case OpNum::CheckWatch: return "CheckWatch";
        case OpNum::RemoveWatch: return "RemoveWatch";
        case OpNum::SetWatch: return "SetWatch";
        case OpNum::SetWatch2: return "SetWatch2";
        case OpNum::TryRemove: return "TryRemove";
        case OpNum::FilteredListWithStatsAndData: return "FilteredListWithStatsAndData";
    }
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
        case OpNum::FilteredListWithStatsAndData:
        case OpNum::Check:
        case OpNum::CheckNotExists:
        case OpNum::CheckWatch:
        case OpNum::CheckStat:
            return "readonly";

        case OpNum::Multi:
            return "multi";

        case OpNum::MultiRead:
            return "multi-read";

        case OpNum::Create:
        case OpNum::Create2:
        case OpNum::Remove:
        case OpNum::TryRemove:
        case OpNum::RemoveWatch:
        case OpNum::SetWatch:
        case OpNum::SetWatch2:
        case OpNum::AddWatch:
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
}

}
