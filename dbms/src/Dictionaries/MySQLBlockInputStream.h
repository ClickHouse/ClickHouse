#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/ExternalResultDescription.h>
#include <mysqlxx/Query.h>
#include <mysqlxx/PoolWithFailover.h>
#include <string>


namespace DB
{

/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IProfilingBlockInputStream
{
public:
    MySQLBlockInputStream(
        const mysqlxx::PoolWithFailover::Entry & entry, const std::string & query_str, const Block & sample_block,
        const size_t max_block_size);

    String getName() const override { return "MySQL"; }

    String getID() const override;

private:
    Block readImpl() override;

    static void insertDefaultValue(IColumn * const column, const IColumn & sample_column)
    {
        column->insertFrom(sample_column, 0);
    }

    mysqlxx::PoolWithFailover::Entry entry;
    mysqlxx::Query query;
    mysqlxx::UseQueryResult result;
    const size_t max_block_size;
    ExternalResultDescription description;
};

}
