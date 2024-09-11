#pragma once

#include <Core/Block.h>
#include <Processors/ISink.h>
#include <Core/ExternalResultDescription.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Interpreters/Context_fwd.h>
#include "ODBCPooledConnectionFactory.h"


namespace DB
{

class ODBCSink final : public ISink
{
using ValueType = ExternalResultDescription::ValueType;

public:
    ODBCSink(
            nanodbc::ConnectionHolderPtr connection_,
            const std::string & remote_database_name_,
            const std::string & remote_table_name_,
            const Block & sample_block_,
            ContextPtr local_context_,
            IdentifierQuotingStyle quoting);

    String getName() const override { return "ODBCSink"; }

protected:
    void consume(Chunk chunk) override;

private:
    LoggerPtr log;

    nanodbc::ConnectionHolderPtr connection_holder;
    std::string db_name;
    std::string table_name;
    Block sample_block;
    ContextPtr local_context;
    IdentifierQuotingStyle quoting;

    ExternalResultDescription description;
};

}
