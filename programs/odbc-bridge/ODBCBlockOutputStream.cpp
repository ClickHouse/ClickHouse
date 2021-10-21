#include "ODBCBlockOutputStream.h"

#include <Common/hex.h>
#include <common/logger_useful.h>
#include <Core/Field.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include "getIdentifierQuote.h"
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Formats/FormatFactory.h>
#include <Parsers/getInsertQuery.h>


namespace DB
{


ODBCBlockOutputStream::ODBCBlockOutputStream(nanodbc::ConnectionHolderPtr connection_holder_,
                                             const std::string & remote_database_name_,
                                             const std::string & remote_table_name_,
                                             const Block & sample_block_,
                                             ContextPtr local_context_,
                                             IdentifierQuotingStyle quoting_)
    : log(&Poco::Logger::get("ODBCBlockOutputStream"))
    , connection_holder(std::move(connection_holder_))
    , db_name(remote_database_name_)
    , table_name(remote_table_name_)
    , sample_block(sample_block_)
    , local_context(local_context_)
    , quoting(quoting_)
{
    description.init(sample_block);
}

Block ODBCBlockOutputStream::getHeader() const
{
    return sample_block;
}

void ODBCBlockOutputStream::write(const Block & block)
{
    WriteBufferFromOwnString values_buf;
    auto writer = FormatFactory::instance().getOutputStream("Values", values_buf, sample_block, local_context);
    writer->write(block);

    std::string query = getInsertQuery(db_name, table_name, block.getColumnsWithTypeAndName(), quoting) + values_buf.str();
    execute<void>(connection_holder,
                  [&](nanodbc::connection & connection) { execute(connection, query); });
}

}
