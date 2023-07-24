//#pragma once
//
//#include <Interpreters/IInterpreter.h>
//
//namespace DB
//{
//
//class InterpreterSelectQueryCoordination : public IInterpreter
//{
//public:
//    using IInterpreterUnionOrSelectQuery::getSampleBlock;
//
//    InterpreterSelectQueryCoordination(
//            const ASTPtr & query_ptr_,
//            ContextPtr context_,
//            const SelectQueryOptions &);
//
//    /** For queries that return a result (SELECT and similar), sets in BlockIO a stream from which you can read this result.
//      * For queries that receive data (INSERT), sets a thread in BlockIO where you can write data.
//      * For queries that do not require data and return nothing, BlockIO will be empty.
//      */
//    BlockIO execute();
//
//    bool ignoreQuota() const { return false; }
//    bool ignoreLimits() const { return false; }
//
//    // Fill query log element with query kind, query databases, query tables and query columns.
//    void extendQueryLogElem(
//            QueryLogElement & elem,
//            const ASTPtr & ast,
//            ContextPtr context,
//            const String & query_database,
//            const String & query_table) const;
//
//    void extendQueryLogElemImpl(QueryLogElement &, const ASTPtr &, ContextPtr) const {}
//
//    /// Returns true if transactions maybe supported for this type of query.
//    /// If Interpreter returns true, than it is responsible to check that specific query with specific Storage is supported.
//    bool supportsTransactions() const { return false; }
//
//    /// Helper function for some Interpreters.
//    static void checkStorageSupportsTransactionsIfNeeded(const StoragePtr & storage, ContextPtr context, bool is_readonly_query = false);
//
//private:
//    ASTPtr query_ptr;
//    ContextMutablePtr context;
//    SelectQueryOptions options;
//};
//
//}
