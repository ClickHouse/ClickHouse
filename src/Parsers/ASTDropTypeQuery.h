#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTDropTypeQuery final : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr database;
    ASTPtr type_name;
    bool if_exists = false;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override;
    QueryKind getQueryKind() const override { return QueryKind::Drop; }

    String getDatabase() const;
    String getTypeName() const;
    void setDatabase(const String & name);
    void normalizeChildrenOrder();

protected:
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
