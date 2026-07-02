#pragma once
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTKillQueryQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    enum class Type : uint8_t
    {
        Query,      /// KILL QUERY
        Mutation,   /// KILL MUTATION
        PartMoveToShard, /// KILL PART_MOVE_TO_SHARD
        Transaction,     /// KILL TRANSACTION
    };

    Type type = Type::Query;
    ASTPtr where_expression;    // expression to filter processes from system.processes table
    bool sync = false;          // SYNC or ASYNC mode
    bool test = false;          // does it TEST mode? (doesn't cancel queries just checks and shows them)

    ASTPtr clone() const override
    {
        auto clone = make_intrusive<ASTKillQueryQuery>(*this);
        clone->children.clear();

        if (where_expression)
            clone->set(clone->where_expression, where_expression->clone());

        cloneOutputOptions(*clone);

        return clone;
    }

    String getID(char) const override;

    /// `getID` folds only `where_expression` (which is also part of `children`) and `sync`; `type`
    /// (`QUERY` / `MUTATION` / `PART_MOVE_TO_SHARD` / `TRANSACTION`), `test`, and the `ON CLUSTER`
    /// `cluster` name are plain members kept outside `children`. Fold them into the tree hash so the
    /// rewrite-rule matcher — which treats an equal tree hash as semantic equality — does not let a
    /// rule written for one `KILL` variant over-match another (for example `KILL QUERY` vs
    /// `KILL MUTATION`, or `... SYNC` vs `... TEST`).
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTKillQueryQuery>(clone());
    }

    QueryKind getQueryKind() const override { return QueryKind::KillQuery; }
};

}
