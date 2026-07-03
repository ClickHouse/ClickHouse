#pragma once
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <unordered_map>


namespace DB
{

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

    using ACLToNumMap = std::unordered_map<Coordination::ACLs, uint64_t, ACLsHash, ACLsComparator>;

    using NumToACLMap = std::unordered_map<uint64_t, Coordination::ACLs>;

    using UsageCounter = std::unordered_map<uint64_t, uint64_t>;

    ACLToNumMap acl_to_num;
    NumToACLMap num_to_acl;
    UsageCounter usage_counter;
    uint64_t max_acl_id{1};

    mutable std::mutex map_mutex;
public:

    /// Convert ACL to number. If it's new ACL than adds it to map
    /// with new id.
    uint64_t convertACLs(const Coordination::ACLs & acls);

    /// Convert number to ACL vector. If number is unknown for map
    /// than throws LOGICAL ERROR
    Coordination::ACLs convertNumber(uint64_t acls_id) const;
    /// Mapping from numbers to ACLs vectors. Used during serialization.
    const NumToACLMap & getMapping() const { return num_to_acl; }

    /// Add mapping to ACLMap. Used during deserialization from snapshot.
    void addMapping(uint64_t acls_id, const Coordination::ACLs & acls);

    /// Add/remove usage of some id. Used to remove unused ACLs.
    void addUsage(uint64_t acl_id);
    void removeUsage(uint64_t acl_id);
};

}
