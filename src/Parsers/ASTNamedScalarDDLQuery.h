#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

#include <Core/UUID.h>
#include <base/types.h>

#include <optional>

namespace DB
{

/// AST for CREATE and DROP NAMED SCALAR.
class ASTNamedScalarDDLQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    enum class Action : uint8_t { Create, Drop };
    enum class CacheKind : uint8_t { Default, Local, Shared };

    Action action = Action::Create;
    CacheKind cache_kind = CacheKind::Default;

    ASTPtr named_scalar_name;

    /// CREATE.
    ASTPtr expression;
    ASTPtr sql_security;
    std::optional<UInt64> refresh_period_seconds;
    bool if_not_exists = false;
    bool or_replace = false;
    UUID uuid = UUIDHelpers::Nil;

    /// DROP.
    bool if_exists = false;

    String getID(char delim) const override
    {
        const char * tag = action == Action::Create ? "CreateScalarQuery" : "DropScalarQuery";
        return String(tag) + (delim + getNamedScalarName());
    }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override
    {
        return removeOnCluster<ASTNamedScalarDDLQuery>(clone());
    }

    String getNamedScalarName() const;

    QueryKind getQueryKind() const override
    {
        return action == Action::Create ? QueryKind::Create : QueryKind::Drop;
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
