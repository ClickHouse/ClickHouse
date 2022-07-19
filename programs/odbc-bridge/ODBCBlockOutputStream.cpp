#include "ODBCBlockOutputStream.h"

#include <Common/hex.h>
#include <common/logger_useful.h>
#include <Core/Field.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include "getIdentifierQuote.h"
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace
{
    using ValueType = ExternalResultDescription::ValueType;

    std::string getInsertQuery(const std::string & db_name, const std::string & table_name, const ColumnsWithTypeAndName & columns, IdentifierQuotingStyle quoting)
    {
        ASTInsertQuery query;
        query.table_id.database_name = db_name;
        query.table_id.table_name = table_name;
        query.columns = std::make_shared<ASTExpressionList>(',');
        query.children.push_back(query.columns);
        for (const auto & column : columns)
            query.columns->children.emplace_back(std::make_shared<ASTIdentifier>(column.name));

        WriteBufferFromOwnString buf;
        IAST::FormatSettings settings(buf, true);
        settings.always_quote_identifiers = true;
        settings.identifier_quoting_style = quoting;
        query.IAST::format(settings);
        return buf.str();
    }
}

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
