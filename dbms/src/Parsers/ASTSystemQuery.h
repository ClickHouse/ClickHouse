#pragma once
#include <Parsers/IAST.h>
#include <AggregateFunctions/AggregateFunctionCount.h>


namespace DB
{

class ASTSystemQuery : public IAST
{
public:

    enum class Type
    {
        _UNKNOWN,
        SHUTDOWN,
        KILL,
        DROP_DNS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        SYNC_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        STOP_MERGES,
        START_MERGES,
        STOP_REPLICATION_QUEUES,
        START_REPLICATION_QUEUES,
        _END
    };

    static const char * typeToString(Type type);

    Type type = Type::_UNKNOWN;

    String target_dictionary;
    //String target_replica_database;
    //String target_replica_table;

    ASTSystemQuery() = default;
    explicit ASTSystemQuery(const StringRange range) : IAST(range) {}

    String getID() const override { return "SYSTEM query"; };

    ASTPtr clone() const override { return std::make_shared<ASTSystemQuery>(*this); }

    ~ASTSystemQuery() override = default;

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}