#include <Parsers/ASTShowIcebergWarehousesQuery.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

ASTPtr ASTShowCreateIcebergWarehouseQuery::clone() const
{
    auto res = make_intrusive<ASTShowCreateIcebergWarehouseQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowCreateIcebergWarehouseQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW CREATE ICEBERG WAREHOUSE " << backQuoteIfNeed(warehouse_name);
}

}
