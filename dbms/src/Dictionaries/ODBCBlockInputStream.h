#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/ExternalResultDescription.h>

#include <Poco/Data/Session.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/RecordSet.h>

#include <string>


namespace DB
{

/// Allows processing results of a query to ODBC source as a sequence of Blocks, simplifies chaining
class ODBCBlockInputStream final : public IProfilingBlockInputStream
{
public:
    ODBCBlockInputStream(
        Poco::Data::Session && session, const std::string & query_str, const Block & sample_block,
        const std::size_t max_block_size);

    String getName() const override { return "ODBC"; }

    String getID() const override;

private:
    Block readImpl() override;

    static void insertDefaultValue(IColumn * const column, const IColumn & sample_column)
    {
        column->insertFrom(sample_column, 0);
    }

    Poco::Data::Session session;
    Poco::Data::Statement statement;
    Poco::Data::RecordSet result;
    Poco::Data::RecordSet::Iterator iterator;

    const std::size_t max_block_size;
    ExternalResultDescription description;

    Poco::Logger * log;
};

}
