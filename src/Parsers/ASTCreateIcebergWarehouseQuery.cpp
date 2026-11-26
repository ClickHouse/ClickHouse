#include <Parsers/ASTCreateIcebergWarehouseQuery.h>

#include <Common/FieldVisitorToString.h>
#include <Common/HiddenSecret.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/formatSettingName.h>

#include <algorithm>


namespace DB
{

bool isIcebergWarehouseSecretSetting(std::string_view name)
{
    return name == "external_id" || name == "role_session_name";
}

void formatIcebergWarehouseSettings(const SettingsChanges & changes, WriteBuffer & ostr, const IAST::FormatSettings & settings)
{
    bool first = true;
    for (const auto & change : changes)
    {
        if (!first)
            ostr << ", ";
        first = false;

        formatSettingName(change.name, ostr);
        if (settings.show_secrets || !isIcebergWarehouseSecretSetting(change.name))
            ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
        else
            ostr << " = " << HIDDEN_SECRET_LITERAL;
    }
}

ASTPtr ASTCreateIcebergWarehouseQuery::clone() const
{
    return make_intrusive<ASTCreateIcebergWarehouseQuery>(*this);
}

bool ASTCreateIcebergWarehouseQuery::hasSecretParts() const
{
    return std::ranges::any_of(changes, [](const auto & change) { return isIcebergWarehouseSecretSetting(change.name); });
}

void ASTCreateIcebergWarehouseQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << "CREATE ICEBERG WAREHOUSE ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(warehouse_name) << " SETTINGS ";
    formatIcebergWarehouseSettings(changes, ostr, settings);
}

}
