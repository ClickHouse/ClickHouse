#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// DROP ICEBERG WAREHOUSE [IF EXISTS] name
class ASTDropIcebergWarehouseQuery : public IAST
{
public:
    String warehouse_name;
    bool if_exists = false;

    String getID(char) const override { return "DropIcebergWarehouseQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
