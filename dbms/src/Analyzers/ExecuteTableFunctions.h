#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <map>


namespace DB
{

class Context;
class WriteBuffer;


/** For every table function, found in first level of the query
  *  (don't go into subqueries)
  *  execute it and save corresponding StoragePtr.
  *
  * Execution of table functions must be done in a stage of query analysis,
  *  because otherwise we don't know table structure. So, it is assumed as cheap operation.
  *
  * Multiple occurences of table functions with same arguments will be executed only once.
  */
struct ExecuteTableFunctions
{
    void process(ASTPtr & ast, const Context & context);

    using Tables = std::map<IAST::Hash, StoragePtr>;
    Tables tables;

    /// Debug output
    void dump(WriteBuffer & out) const;
};

}
