#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <memory>
#include <string>
#include <variant>

#include <base/types.h>

#include <Common/ZooKeeper/IKeeper.h>
#include <Coordination/ACLMap.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/Storage/Node.h>

namespace DB
{

// Applying ZooKeeper request to storage consists of two steps:
//  - preprocessing which, instead of applying the changes directly to storage,
//    generates deltas with those changes, denoted with the request ZXID
//  - processing which applies deltas with the correct ZXID to the storage
//
// Delta objects allow us two things:
//  - fetch the latest, uncommitted state of an object by getting the committed
//    state of that same object from the storage and applying the deltas
//    in the same order as they are defined
//  - quickly commit the changes to the storage

struct LSMTDelta
{
    /// Pin new_node's block to keep the data pointer valid.
    /// (Probably not strictly necessary because the data lives in an uncommitted memtable, so
    ///  shouldn't be invalidated anyway, but feels less sketchy this way.)
    Coordination::Storage::NodeRef new_node_ref;
    /// Deserialized node corresponding to new_node_ref.
    /// Has path depth and hash (even if action is Remove), but the path string is invalid and needs
    /// to be taken from Delta before use.
    Coordination::Storage::FullNode new_node;
    /// (In contrast, rollback speed is not important, so not wasting bytes on FullNode here.)
    Coordination::Storage::NodeRef old_node_ref;
    uint64_t old_digest = 0;

    ACLId old_acl_id = 0;
    int64_t ephemeral_owner = 0;
};

struct CreateNodeDelta
{
    Coordination::Stat stat;
    ACLId acl_id;
    String data;
};

struct RemoveNodeDelta
{
    KeeperNodeStats stat;
    String data;
};

struct UpdateNodeStatDelta
{
    explicit UpdateNodeStatDelta(const KeeperNodeStats & stats)
        : old_stats(stats), new_stats(stats) {}

    KeeperNodeStats old_stats;
    KeeperNodeStats new_stats;
};

struct UpdateNodeDataDelta
{
    std::string old_data;
    std::string new_data;
};

struct ErrorDelta
{
    Coordination::Error error;
};

struct FailedMultiDelta
{
    size_t failed_pos = std::numeric_limits<size_t>::max();
    Coordination::Error failed_pos_error = Coordination::Error::ZOK;
    Coordination::Error global_error = Coordination::Error::ZOK;
};

// Denotes end of a subrequest in multi request
struct SubDeltaEnd
{
};

struct AddAuthDelta
{
    int64_t session_id;
    std::shared_ptr<KeeperAuthID> auth_id;
};

struct CloseSessionDelta
{
    int64_t session_id;
};

struct KeeperDelta
{
    using Operation = std::variant<
        /// KeeperLSMTNodesStorage delta (just one type: overwrite whole node).
        LSMTDelta,

        /// KeeperMemNodesStorage deltas.
        CreateNodeDelta,
        RemoveNodeDelta,
        UpdateNodeStatDelta,
        UpdateNodeDataDelta,

        /// Other deltas are handled by base KeeperStorage.
        AddAuthDelta,
        ErrorDelta,
        SubDeltaEnd,
        FailedMultiDelta,
        CloseSessionDelta>;

    KeeperDelta(String path_, int64_t zxid_, Operation operation_) : path(std::move(path_)), zxid(zxid_), operation(std::move(operation_)) { }

    KeeperDelta(int64_t zxid_, Coordination::Error error) : KeeperDelta("", zxid_, ErrorDelta{error}) { }

    KeeperDelta(int64_t zxid_, Operation subdelta) : KeeperDelta("", zxid_, subdelta) { }

    String path;
    int64_t zxid;
    Operation operation;
};

/// For the duration of a preprocessRequest call, these fields accumulate changes made by the
/// transaction that's being preprocessed. These deltas are already applied to uncommitted stats;
/// if the transaction fails, they must be rolled back.
struct KeeperStagingTransaction
{
    int64_t zxid = -1;
    KeeperDigest digest;
    std::list<KeeperDelta> deltas;
};

}
