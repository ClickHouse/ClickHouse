#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Interpreters of statements for warehouses of the Iceberg REST catalog.
/// They use `IcebergRESTCatalogWarehouseRegistry`, like the `/management/v1/warehouses` routes of the catalog server.

class InterpreterCreateIcebergWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateIcebergWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

class InterpreterDropIcebergWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropIcebergWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
