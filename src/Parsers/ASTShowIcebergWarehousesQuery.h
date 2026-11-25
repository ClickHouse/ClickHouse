#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

struct ASTShowIcebergWarehousesIDAndQueryNames
{
    static constexpr auto ID = "ShowIcebergWarehousesQuery";
    static constexpr auto Query = "SHOW ICEBERG WAREHOUSES";
    static constexpr auto Kind = IAST::QueryKind::Show;
};

/// SHOW ICEBERG WAREHOUSES
using ASTShowIcebergWarehousesQuery = ASTQueryWithOutputImpl<ASTShowIcebergWarehousesIDAndQueryNames>;

/// SHOW CREATE ICEBERG WAREHOUSE name
class ASTShowCreateIcebergWarehouseQuery : public ASTQueryWithOutput
{
public:
    String warehouse_name;

    String getID(char) const override { return "ShowCreateIcebergWarehouseQuery"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
