#pragma once

#include <string_view>

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Common/Scheduler/CostUnit.h>
#include <Common/Scheduler/ResourceAccessMode.h>

namespace DB
{

class ASTCreateResourceQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    /// Describes specific operation that requires this resource
    struct Operation
    {
        ResourceAccessMode mode{};
        std::optional<String> disk; // Applies to all disks if not set
        CostUnit unit() const { return costUnitForMode(mode); }

        friend bool operator ==(const Operation & lhs, const Operation & rhs) { return lhs.mode == rhs.mode && lhs.disk == rhs.disk; }
        friend bool operator !=(const Operation & lhs, const Operation & rhs) { return !(lhs == rhs); }
    };

    using Operations = std::vector<Operation>;

    ASTPtr resource_name;
    Operations operations; /// List of operations that require this resource

    CostUnit unit{}; /// What cost units are managed by the resource

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateResourceQuery" + (delim + getResourceName()); }

    ASTPtr clone() const override;

    /// `resource_name` lives in `children`, but `operations` / `or_replace` / `if_not_exists` /
    /// the `ON CLUSTER` name are plain members. Without folding them into the hash,
    /// `CREATE RESOURCE r (READ DISK d)` and `CREATE RESOURCE r (WRITE DISK d)` would share one
    /// tree hash. The rewrite-rule matcher treats an equal tree hash as semantic equality, so a
    /// rule template for one `CREATE RESOURCE` would over-match another. `unit` is derived from
    /// `operations` and never emitted by the formatter, so it is deliberately not folded.
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTCreateResourceQuery>(clone()); }

    String getResourceName() const;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState & state, FormatStateStacked frame) const override;
};

}
