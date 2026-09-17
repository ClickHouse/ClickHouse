#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

#include <string_view>
#include <utility>


namespace DB
{

class ASTPhysicalizeTypeReferencesQuery final : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    enum class Scope : UInt8
    {
        Object,
        DependentClosure,
        Database,
    };

    Scope scope = Scope::Object;
    ASTPtr object_kind;
    ASTPtr database;
    ASTPtr object_name;
    bool drop_unused_types = false;

    String getID(char) const override { return "PhysicalizeTypeReferencesQuery"; }
    ASTPtr clone() const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams & params) const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

    String getDatabase() const;
    String getObjectName() const;
    void setDatabase(const String & name);
    void normalizeChildrenOrder();

protected:
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

class ASTApplyPhysicalizeTypeReferencesQuery final : public IAST
{
public:
    static constexpr std::string_view redacted_token = "<redacted>";

    String getID(char) const override { return "ApplyPhysicalizeTypeReferencesQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Alter; }
    bool hasSecretParts() const override { return true; }

    const String & getToken() const { return token; }
    void setToken(String value) { token = std::move(value); }

protected:
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;
    void formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override;

private:
    String token;
};

}
