#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <functional>

namespace DB
{

class ITableFunction;
using TableFunctionPtr = std::shared_ptr<ITableFunction>;
struct StorageID;

struct TableFunctionDelegate
{
    TableFunctionPtr table_function;
    StoragePtr storage;
};

/// Creates the storage in read mode or in write mode, and checks the source access of that mode.
using ResolveTableFunctionDelegate = std::function<TableFunctionDelegate(const ContextPtr & context, bool is_insert_query)>;

/// A table of a database whose tables are table function calls (`URL`, `S3`): resolved in read mode, re-resolved in write mode
/// by every write. Returns nullptr when the read-mode resolution creates no storage.
StoragePtr makeTableFunctionDatabaseTable(const StorageID & storage_id, const ResolveTableFunctionDelegate & resolve, const ContextPtr & context);

}
