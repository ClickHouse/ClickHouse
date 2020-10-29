#pragma once

#include <Parsers/New/AST/Query.h>


namespace DB::AST
{

class SystemQuery : public Query
{
    public:
        static PtrTo<SystemQuery> createDistributedSends(bool stop, PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createFetches(bool stop, PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createFlushDistributed(PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createFlushLogs();
        static PtrTo<SystemQuery> createMerges(bool stop, PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createSyncReplica(PtrTo<TableIdentifier> identifier);

        ASTPtr convertToOld() const override;

    private:
        enum ChildIndex : UInt8
        {
            TABLE = 0,
        };
        enum class QueryType
        {
            DISTRIBUTED_SENDS,
            FETCHES,
            FLUSH_DISTRIBUTED,
            FLUSH_LOGS,
            MERGES,
            SYNC_REPLICA,
        };

        QueryType query_type;
        bool stop = false;

        SystemQuery(QueryType type, PtrList exprs);
};

}
