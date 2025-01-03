#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Access/Common/AccessType.h>
#include <Common/FunctionDocumentation.h>
#include <Analyzer/IQueryTreeNode.h>

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
    static std::string getDatabaseName() { return "_table_function"; }

    /// Get the main function name.
    virtual std::string getName() const = 0;

    /// Returns true if we always know table structure when executing table function
    /// (e.g. structure is specified in table function arguments)
    virtual bool hasStaticStructure() const { return false; }
    /// Returns false if storage returned by table function supports type conversion (e.g. StorageDistributed)
    virtual bool needStructureConversion() const { return true; }

    /** Return array of table function arguments indexes for which query tree analysis must be skipped.
      * It is important for table functions that take subqueries, because otherwise analyzer will resolve them.
      */
    virtual std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & /*query_node_table_function*/, ContextPtr /*context*/) const { return {}; }

    virtual void parseArguments(const ASTPtr & /*ast_function*/, ContextPtr /*context*/) {}

    /// Returns actual table structure probably requested from remote server, may fail
    virtual ColumnsDescription getActualTableStructure(ContextPtr /*context*/, bool is_insert_query) const = 0;

    /// Check if table function needs a structure hint from SELECT query in case of
    /// INSERT INTO FUNCTION ... SELECT ... and INSERT INTO ... SELECT ... FROM table_function(...)
    /// It's used for schema inference.
    virtual bool needStructureHint() const { return false; }

    /// Set a structure hint from SELECT query in case of
    /// INSERT INTO FUNCTION ... SELECT ... and INSERT INTO ... SELECT ... FROM table_function(...)
    /// This hint could be used not to repeat schema in function arguments.
    virtual void setStructureHint(const ColumnsDescription &) {}

    /// Used for table functions that can use structure hint during INSERT INTO ... SELECT ... FROM table_function(...)
    /// It returns possible virtual column names of corresponding storage. If select query contains
    /// one of these columns, the structure from insertion table won't be used as a structure hint,
    /// because we cannot determine which column from table correspond to this virtual column.
    virtual std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const { return {}; }

    virtual bool supportsReadingSubsetOfColumns(const ContextPtr &) { return true; }

    /// Create storage according to the query.
    StoragePtr
    execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns_ = {}, bool use_global_context = false, bool is_insert = false) const;

    virtual ~ITableFunction() = default;

protected:
    virtual AccessType getSourceAccessType() const;

private:
    virtual StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const = 0;

    virtual const char * getStorageTypeName() const = 0;
};

/// Properties of table function that are independent of argument types and parameters.
struct TableFunctionProperties
{
    FunctionDocumentation documentation;

    /** It is determined by the possibility of modifying any data or making requests to arbitrary hostnames.
      *
      * If users can make a request to an arbitrary hostname, they can get the info from the internal network
      * or manipulate internal APIs (say - put some data into Memcached, which is available only in the corporate network).
      * This is named "SSRF attack".
      * Or a user can use an open ClickHouse server to amplify DoS attacks.
      *
      * In those cases, the table function should not be allowed in readonly mode.
      */
    bool allow_readonly = false;
};


using TableFunctionPtr = std::shared_ptr<ITableFunction>;


}
