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
        static PtrTo<SystemQuery> createReloadDictionaries();
        static PtrTo<SystemQuery> createReloadDictionary(PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createReplicatedSends(bool stop);
        static PtrTo<SystemQuery> createSyncReplica(PtrTo<TableIdentifier> identifier);
        static PtrTo<SystemQuery> createTTLMerges(bool stop, PtrTo<TableIdentifier> identifier);

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
            RELOAD_DICTIONARIES,
            RELOAD_DICTIONARY,
            REPLICATED_SENDS,
            SYNC_REPLICA,
            TTL_MERGES,
        };

        QueryType query_type;
        bool stop = false;

        SystemQuery(QueryType type, PtrList exprs);
};

}
