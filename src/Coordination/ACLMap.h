#pragma once
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <unordered_map>


namespace DB
{

using ACLId = uint32_t;

/// Simple mapping of different ACLs to sequentially growing numbers
/// Allows to store single number instead of vector of ACLs on disk and in memory.
class ACLMap
{
private:
    struct ACLsHash
    {
        size_t operator()(const Coordination::ACLs & acls) const;
    };

    struct ACLsComparator
    {
        bool operator()(const Coordination::ACLs & left, const Coordination::ACLs & right) const;
    };

    struct MapEntry
    {
        Coordination::ACLs acls;
        std::atomic<uint64_t> usage;

        MapEntry(Coordination::ACLs acls_, uint64_t usage_) : acls(std::move(acls_)), usage(usage_) {}
    };

    using ACLToNumMap = std::unordered_map<Coordination::ACLs, ACLId, ACLsHash, ACLsComparator>;

    /// Note: maybe we should change it to std::vector and reuse ids more aggressively to keep it dense.
    using NumToACLMap = std::unordered_map<ACLId, MapEntry>;

    ACLToNumMap acl_to_num;
    NumToACLMap num_to_acl;
    ACLId max_acl_id{1};

    mutable SharedMutex map_mutex;

    MapEntry & numToAcl(ACLId id); // like num_to_acl.at(id), but with better exception on error
public:

    /// Convert ACL to number. If it's new ACL than adds it to map with new id.
    /// Increments usage counter for the returned id (no need to call addUsage after this).
    /// Be careful to not discard the returned id without calling removeUsage;
    /// in particular, make sure nothing can fail after convertACLs call but before
    /// storing the ACLId somewhere (e.g. in a list of Delta-s that would be rolled back on error).
    ACLId convertACLs(const Coordination::ACLs & acls);

    /// Convert number to ACL vector. If number is unknown for map
    /// than throws LOGICAL ERROR
    Coordination::ACLs convertNumber(ACLId acls_id) const;
    /// Makes a copy of the mapping from numbers to ACLs vectors. Used during serialization.
    std::vector<std::pair<ACLId, Coordination::ACLs>> getMapping() const;

    /// Add mapping to ACLMap. Used during deserialization from snapshot.
    /// Does not increment usage counter, it starts at 0.
    void addMapping(ACLId acls_id, const Coordination::ACLs & acls);

    /// Add/remove usage of some id. Used to remove unused ACLs.
    void addUsage(ACLId acl_id);
    void removeUsage(ACLId acl_id);

    /// Remove all mappings whose usage counter is 0. Used after snapshot deserialization
    /// to drop ACLs that are present in the snapshot's ACL map but not referenced by any node.
    void removeUnusedACLs();
};

}
