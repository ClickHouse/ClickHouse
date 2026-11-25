#pragma once

#include <Parsers/IAST.h>
#include <Common/SettingsChanges.h>


namespace DB
{

/// ALTER ICEBERG WAREHOUSE name [SET role_arn = '...' [, ...]] [DELETE external_id [, ...]]
class ASTAlterIcebergWarehouseQuery : public IAST
{
public:
    String warehouse_name;
    SettingsChanges changes;
    std::vector<String> delete_keys;

    String getID(char) const override { return "AlterIcebergWarehouseQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Alter; }

    bool hasSecretParts() const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
