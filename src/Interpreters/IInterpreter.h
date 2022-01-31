#pragma once

#include <QueryPipeline/BlockIO.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages//IStorage_fwd.h>

namespace DB
{

struct QueryLogElement;

/** Interpreters interface for different queries.
  */
class IInterpreter
{
public:
    /** For queries that return a result (SELECT and similar), sets in BlockIO a stream from which you can read this result.
      * For queries that receive data (INSERT), sets a thread in BlockIO where you can write data.
      * For queries that do not require data and return nothing, BlockIO will be empty.
      */
    virtual BlockIO execute() = 0;

    virtual bool ignoreQuota() const { return false; }
    virtual bool ignoreLimits() const { return false; }

    // Fill query log element with query kind, query databases, query tables and query columns.
    void extendQueryLogElem(
        QueryLogElement & elem,
        const ASTPtr & ast,
        ContextPtr context,
        const String & query_database,
        const String & query_table) const;

    virtual void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const {}

    /// Returns true if transactions maybe supported for this type of query.
    /// If Interpreter returns true, than it is responsible to check that specific query with specific Storage is supported.
    virtual bool supportsTransactions() const { return false; }

    /// Helper function for some Interpreters.
    static void checkStorageSupportsTransactionsIfNeeded(const StoragePtr & storage, ContextPtr context);

    virtual ~IInterpreter() = default;
};

}
