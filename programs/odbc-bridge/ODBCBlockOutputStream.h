#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Core/ExternalResultDescription.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Interpreters/Context_fwd.h>
#include "ODBCConnectionFactory.h"


namespace DB
{

class ODBCBlockOutputStream : public IBlockOutputStream
{

public:
    ODBCBlockOutputStream(
            nanodbc::ConnectionHolderPtr connection_,
            const std::string & remote_database_name_,
            const std::string & remote_table_name_,
            const Block & sample_block_,
            ContextPtr local_context_,
            IdentifierQuotingStyle quoting);

    Block getHeader() const override;
    void write(const Block & block) override;

private:
    Poco::Logger * log;

    nanodbc::ConnectionHolderPtr connection_holder;
    std::string db_name;
    std::string table_name;
    Block sample_block;
    ContextPtr local_context;
    IdentifierQuotingStyle quoting;

    ExternalResultDescription description;
};

}
