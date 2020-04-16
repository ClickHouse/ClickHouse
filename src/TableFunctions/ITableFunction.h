#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>

#include <memory>
#include <string>


namespace DB
{

class Context;

/** Interface for table functions.
  *
  * Table functions are not relevant to other functions.
  * The table function can be specified in the FROM section instead of the [db.]Table
  * The table function returns a temporary StoragePtr object that is used to execute the query.
  *
  * Example:
  * SELECT count() FROM remote('example01-01-1', merge, hits)
  * - go to `example01-01-1`, in `merge` database, `hits` table.
  */

class ITableFunction
{
public:
    static inline std::string getDatabaseName() { return "_table_function"; }

    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Create storage according to the query.
    StoragePtr execute(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const;

    virtual ~ITableFunction() {}

private:
    virtual StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const = 0;
    virtual const char * getStorageTypeName() const = 0;
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
