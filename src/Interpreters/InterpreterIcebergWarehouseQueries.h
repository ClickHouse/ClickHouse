#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Interpreters of statements for warehouses of the Iceberg REST catalog.
/// They call the same `IcebergRESTCatalogWarehouseRegistry` as the `/management/v1/warehouses` routes of the catalog server.

class InterpreterCreateIcebergWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateIcebergWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

class InterpreterAlterIcebergWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterAlterIcebergWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

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

/// Returns one column `name`, sorted. Needs only `SHOW ICEBERG WAREHOUSES`, with no `SELECT` grant on `system.iceberg_warehouses`.
class InterpreterShowIcebergWarehousesQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowIcebergWarehousesQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

/// Returns one row with one column `statement`.
class InterpreterShowCreateIcebergWarehouseQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowCreateIcebergWarehouseQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
