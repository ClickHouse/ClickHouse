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

    using ACLToNumMap = std::unordered_map<Coordination::ACLs, ACLId, ACLsHash, ACLsComparator>;

    using NumToACLMap = std::unordered_map<ACLId, Coordination::ACLs>;

    using UsageCounter = std::unordered_map<ACLId, uint64_t>;

    ACLToNumMap acl_to_num;
    NumToACLMap num_to_acl;
    UsageCounter usage_counter;
    ACLId max_acl_id{1};

    mutable std::mutex map_mutex;
public:

    /// Convert ACL to number. If it's new ACL than adds it to map
    /// with new id.
    ACLId convertACLs(const Coordination::ACLs & acls);

    /// Convert number to ACL vector. If number is unknown for map
    /// than throws LOGICAL ERROR
    Coordination::ACLs convertNumber(ACLId acls_id) const;
    /// Mapping from numbers to ACLs vectors. Used during serialization.
    const NumToACLMap & getMapping() const { return num_to_acl; }

    /// Add mapping to ACLMap. Used during deserialization from snapshot.
    void addMapping(ACLId acls_id, const Coordination::ACLs & acls);

    /// Add/remove usage of some id. Used to remove unused ACLs.
    void addUsage(ACLId acl_id);
    void removeUsage(ACLId acl_id);
};

}
