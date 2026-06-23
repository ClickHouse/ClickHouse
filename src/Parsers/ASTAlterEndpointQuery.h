#pragma once

#include <Parsers/IAST.h>

#include <Common/SettingsChanges.h>


namespace DB
{

/// `ALTER ENDPOINT` — modify SQL-managed cluster metadata endpoint properties.
class ASTAlterEndpointQuery : public IAST
{
public:
    String endpoint_name;
    SettingsChanges properties;

    String getID(char) const override { return "AlterEndpointQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
