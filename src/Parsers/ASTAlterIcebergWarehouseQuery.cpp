#include <Parsers/ASTAlterIcebergWarehouseQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateIcebergWarehouseQuery.h>

#include <algorithm>


namespace DB
{

ASTPtr ASTAlterIcebergWarehouseQuery::clone() const
{
    return make_intrusive<ASTAlterIcebergWarehouseQuery>(*this);
}

bool ASTAlterIcebergWarehouseQuery::hasSecretParts() const
{
    return std::ranges::any_of(changes, [](const auto & change) { return isIcebergWarehouseSecretSetting(change.name); });
}

void ASTAlterIcebergWarehouseQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << "ALTER ICEBERG WAREHOUSE " << backQuoteIfNeed(warehouse_name);
    if (!changes.empty())
    {
        ostr << " SET ";
        formatIcebergWarehouseSettings(changes, ostr, settings);
    }
    if (!delete_keys.empty())
    {
        ostr << " DELETE ";
        bool first = true;
        for (const auto & key : delete_keys)
        {
            if (!first)
                ostr << ", ";
            first = false;
            ostr << backQuoteIfNeed(key);
        }
    }
}

}
