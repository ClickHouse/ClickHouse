#include <Parsers/ASTDropIcebergWarehouseQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTDropIcebergWarehouseQuery::clone() const
{
    return make_intrusive<ASTDropIcebergWarehouseQuery>(*this);
}

void ASTDropIcebergWarehouseQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "DROP ICEBERG WAREHOUSE ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(warehouse_name);
}

}
