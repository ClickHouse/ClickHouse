#pragma once

#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>

#include <vector>


namespace DB
{

struct ASTSQLClusterReplica : public IAST
{
    SettingsChanges properties;

    String getID(char) const override { return "SQLClusterReplica"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

struct ASTSQLClusterShard : public IAST
{
    SettingsChanges properties;
    std::vector<ASTPtr> replicas;

    String getID(char) const override { return "SQLClusterShard"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

/// Shared body for `CREATE CLUSTER` and `ALTER CLUSTER`.
struct ASTSQLClusterDefinition : public IAST
{
    SettingsChanges cluster_properties;
    std::vector<ASTPtr> shards;

    String getID(char) const override { return "SQLClusterDefinition"; }
    ASTPtr clone() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

class ASTCreateSQLClusterQuery : public IAST
{
public:
    String cluster_name;
    ASTPtr definition;
    bool if_not_exists = false;

    String getID(char) const override { return "CreateSQLClusterQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

class ASTAlterSQLClusterQuery : public IAST
{
public:
    String cluster_name;
    ASTPtr definition;
    bool if_exists = false;

    String getID(char) const override { return "AlterSQLClusterQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

class ASTDropSQLClusterQuery : public IAST
{
public:
    String cluster_name;
    bool if_exists = false;

    String getID(char) const override { return "DropSQLClusterQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

void formatSQLClusterPropertiesList(
    WriteBuffer & ostr,
    const SettingsChanges & properties,
    const IAST::FormatSettings & settings,
    IAST::FormatState & state,
    IAST::FormatStateStacked frame);

}
