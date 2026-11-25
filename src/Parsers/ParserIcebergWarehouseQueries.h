#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Statements for warehouses of the Iceberg REST catalog.

/// CREATE ICEBERG WAREHOUSE [IF NOT EXISTS] name SETTINGS base_location = '...', role_arn = '...' [, ...]
class ParserCreateIcebergWarehouseQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "CREATE ICEBERG WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// ALTER ICEBERG WAREHOUSE name [SET role_arn = '...' [, ...]] [DELETE external_id [, ...]]
class ParserAlterIcebergWarehouseQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "ALTER ICEBERG WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// DROP ICEBERG WAREHOUSE [IF EXISTS] name
class ParserDropIcebergWarehouseQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "DROP ICEBERG WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// SHOW ICEBERG WAREHOUSES
class ParserShowIcebergWarehousesQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "SHOW ICEBERG WAREHOUSES query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// SHOW CREATE ICEBERG WAREHOUSE name
class ParserShowCreateIcebergWarehouseQuery : public IParserBase
{
protected:
    [[nodiscard]] const char * getName() const override { return "SHOW CREATE ICEBERG WAREHOUSE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
