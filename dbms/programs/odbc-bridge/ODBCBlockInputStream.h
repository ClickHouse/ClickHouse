#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Poco/Data/RecordSet.h>
#include <Poco/Data/Session.h>
#include <Poco/Data/Statement.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{
/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IBlockInputStream
{
public:
    ODBCBlockInputStream(
        Poco::Data::Session && session, const std::string & query_str, const Block & sample_block, const UInt64 max_block_size);

    String getName() const override { return "ODBC"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    Poco::Data::Session session;
    Poco::Data::Statement statement;
    Poco::Data::RecordSet result;
    Poco::Data::RecordSet::Iterator iterator;

    const UInt64 max_block_size;
    ExternalResultDescription description;

    Poco::Logger * log;
};

}
