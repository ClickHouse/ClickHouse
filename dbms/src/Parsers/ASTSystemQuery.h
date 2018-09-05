#pragma once

#include <Common/config.h>
#include <Parsers/IAST.h>


namespace DB
{

class ASTSystemQuery : public IAST
{
public:

    enum class Type
    {
        UNKNOWN,
        SHUTDOWN,
        KILL,
        DROP_DNS_CACHE,
        DROP_MARK_CACHE,
        DROP_UNCOMPRESSED_CACHE,
#if USE_EMBEDDED_COMPILER
        DROP_COMPILED_EXPRESSION_CACHE,
#endif
        STOP_LISTEN_QUERIES,
        START_LISTEN_QUERIES,
        RESTART_REPLICAS,
        RESTART_REPLICA,
        SYNC_REPLICA,
        RELOAD_DICTIONARY,
        RELOAD_DICTIONARIES,
        RELOAD_EMBEDDED_DICTIONARIES,
        RELOAD_CONFIG,
        STOP_MERGES,
        START_MERGES,
        STOP_FETCHES,
        START_FETCHES,
        STOP_REPLICATED_SENDS,
        START_REPLICATEDS_SENDS,
        STOP_REPLICATION_QUEUES,
        START_REPLICATION_QUEUES,
        FLUSH_SYSTEM_TABLES,
        END
    };

    static const char * typeToString(Type type);

    Type type = Type::UNKNOWN;

    String target_dictionary;
    String target_database;
    String target_table;

    String getID() const override { return "SYSTEM query"; }

    ASTPtr clone() const override { return std::make_shared<ASTSystemQuery>(*this); }

protected:

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};


}
