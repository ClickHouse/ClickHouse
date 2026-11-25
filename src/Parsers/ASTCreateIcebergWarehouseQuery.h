#pragma once

#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>


namespace DB
{

/// Settings of `CREATE ICEBERG WAREHOUSE` and `ALTER ICEBERG WAREHOUSE` whose values are hidden in logs and in `SHOW CREATE`.
bool isIcebergWarehouseSecretSetting(std::string_view name);

/// Writes `name = value, ...` and hides values of secret settings unless `show_secrets` is set.
void formatIcebergWarehouseSettings(const SettingsChanges & changes, WriteBuffer & ostr, const IAST::FormatSettings & settings);

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
};

}
