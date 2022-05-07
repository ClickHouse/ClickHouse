#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Access/Common/AccessType.h>

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
  *
  *
  * When creating table AS table_function(...) we probably don't know structure of the table
  * and have to request if from remote server, because structure is required to create a Storage.
  * To avoid failures on server startup, we write obtained structure to metadata file.
  * So, table function may have two different columns lists:
  *  - cached_columns written to metadata
  *  - the list returned from getActualTableStructure(...)
  * See StorageTableFunctionProxy.
  */

class ITableFunction : public std::enable_shared_from_this<ITableFunction>
{
public:
    static inline std::string getDatabaseName() { return "_table_function"; }

    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Returns true if we always know table structure when executing table function
    /// (e.g. structure is specified in table function arguments)
    virtual bool hasStaticStructure() const { return false; }
    /// Returns false if storage returned by table function supports type conversion (e.g. StorageDistributed)
    virtual bool needStructureConversion() const { return true; }

    virtual void parseArguments(const ASTPtr & /*ast_function*/, ContextPtr /*context*/) {}

    /// Returns actual table structure probably requested from remote server, may fail
    virtual ColumnsDescription getActualTableStructure(ContextPtr /*context*/) const = 0;

    /// Check if table function needs a structure hint from SELECT query in case of
    /// INSERT INTO FUNCTION ... SELECT ...
    /// It's used for schema inference.
    virtual bool needStructureHint() const { return false; }

    /// Set a structure hint from SELECT query in case of
    /// INSERT INTO FUNCTION ... SELECT ...
    /// This hint could be used not to repeat schema in function arguments.
    virtual void setStructureHint(const ColumnsDescription &) {}

    /// Create storage according to the query.
    StoragePtr
    execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns_ = {}, bool use_global_context = false) const;

    virtual ~ITableFunction() = default;

private:
    virtual StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const = 0;

    virtual const char * getStorageTypeName() const = 0;

    virtual AccessType getSourceAccessType() const;
};

using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
