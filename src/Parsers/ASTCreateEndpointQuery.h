#pragma once

#include <Parsers/IAST.h>

#include <Common/SettingsChanges.h>


namespace DB
{

/// `CREATE ENDPOINT` — SQL-managed cluster metadata connection parameters.
class ASTCreateEndpointQuery : public IAST
{
public:
    String endpoint_name;
    SettingsChanges properties;
    bool if_not_exists = false;
    /// Optional trailing `SYNC` — wait for replica-group catalog apply and return status rows.
    bool sync = false;

    String getID(char) const override { return "CreateEndpointQuery"; }

    ASTPtr clone() const override;

    /// `PROPERTIES` may hold endpoint passwords or cluster secrets; they are kept outside the AST children,
    /// so advertise them explicitly to make `formatForLogging` mask them in logs and `system.query_log`.
    bool hasSecretParts() const override { return true; }

    QueryKind getQueryKind() const override { return QueryKind::Create; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
