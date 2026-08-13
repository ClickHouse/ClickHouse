#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// `DROP ENDPOINT` — remove SQL-managed cluster metadata endpoint.
class ASTDropEndpointQuery : public IAST
{
public:
    String endpoint_name;
    bool if_exists = false;
    /// Optional trailing `SYNC` — wait for replica-group catalog apply and return status rows.
    bool sync = false;

    String getID(char) const override { return "DropEndpointQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
