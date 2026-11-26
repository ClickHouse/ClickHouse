#pragma once

#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>


namespace DB
{

/// CREATE ICEBERG WAREHOUSE [IF NOT EXISTS] name SETTINGS base_location = '...', role_arn = '...' [, ...]
class ASTCreateIcebergWarehouseQuery : public IAST
{
public:
    String warehouse_name;
    SettingsChanges changes;
    bool if_not_exists = false;

    String getID(char) const override { return "CreateIcebergWarehouseQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Create; }

    bool hasSecretParts() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

private:
    /// Values of these settings are hidden in logs.
    static bool isSecretSetting(std::string_view name);
};

}
