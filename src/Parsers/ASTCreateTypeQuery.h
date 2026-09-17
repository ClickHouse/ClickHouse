#pragma once

#include <Core/UUID.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTUDTTemplate.h>
#include <Parsers/IAST.h>

#include <optional>


namespace DB
{

class ASTCreateTypeQuery final : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr database;
    ASTPtr type_name;
    ASTPtr parameters;
    ASTPtr decreases;
    ASTPtr definition;
    ASTPtr comment;

    bool attach = false;
    bool if_not_exists = false;

    std::optional<UUID> uuid;
    std::optional<UInt64> revision;
    std::optional<String> definition_hash;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override;
    QueryKind getQueryKind() const override { return QueryKind::Create; }

    String getDatabase() const;
    String getTypeName() const;
    void setDatabase(const String & name);
    void normalizeChildrenOrder();

protected:
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
