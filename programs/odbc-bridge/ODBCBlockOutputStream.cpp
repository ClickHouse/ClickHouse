#include "ODBCBlockOutputStream.h"

#include <base/logger_useful.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Parsers/getInsertQuery.h>


namespace DB
{


ODBCSink::ODBCSink(
    nanodbc::ConnectionHolderPtr connection_holder_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const Block & sample_block_,
    ContextPtr local_context_,
    IdentifierQuotingStyle quoting_)
    : ISink(sample_block_)
    , log(&Poco::Logger::get("ODBCSink"))
    , connection_holder(std::move(connection_holder_))
    , db_name(remote_database_name_)
    , table_name(remote_table_name_)
    , sample_block(sample_block_)
    , local_context(local_context_)
    , quoting(quoting_)
{
    description.init(sample_block);
}


void ODBCSink::consume(Chunk chunk)
{
    auto block = getPort().getHeader().cloneWithColumns(chunk.detachColumns());
    WriteBufferFromOwnString values_buf;
    auto writer = local_context->getOutputFormat("Values", values_buf, sample_block);
    writer->write(block);

    std::string query = getInsertQuery(db_name, table_name, block.getColumnsWithTypeAndName(), quoting) + values_buf.str();
    execute<void>(connection_holder,
        [&](nanodbc::connection & connection) { execute(connection, query); });
}

}
