#pragma once

#include <Parsers/IAST_fwd.h>

#include <string>
#include <memory>


namespace DB
{

class Context;
class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;


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
    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Create storage according to the query.
    StoragePtr execute(const ASTPtr & ast_function, const Context & context) const;

    virtual ~ITableFunction() {}

private:
    virtual StoragePtr executeImpl(const ASTPtr & ast_function, const Context & context) const = 0;
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
