#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/ExternalResultDescription.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <Poco/Data/Session.h>
    #include <Poco/Data/Statement.h>
    #include <Poco/Data/RecordSet.h>
#pragma GCC diagnostic pop

#include <string>


namespace DB
{

/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IProfilingBlockInputStream
{
public:
    ODBCBlockInputStream(
        Poco::Data::Session && session, const std::string & query_str, const Block & sample_block,
        const size_t max_block_size);

    String getName() const override { return "ODBC"; }

    Block getHeader() const override { return description.sample_block; };

private:
    Block readImpl() override;

    Poco::Data::Session session;
    Poco::Data::Statement statement;
    Poco::Data::RecordSet result;
    Poco::Data::RecordSet::Iterator iterator;

    const size_t max_block_size;
    ExternalResultDescription description;

    Poco::Logger * log;
};

}
