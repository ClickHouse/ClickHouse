#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <mysqlxx/PoolWithFailover.h>
#include <mysqlxx/Query.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{
/// Allows processing results of a MySQL query as a sequence of Blocks, simplifies chaining
class MySQLBlockInputStream final : public IBlockInputStream
{
public:
    MySQLBlockInputStream(
        const mysqlxx::PoolWithFailover::Entry & entry_,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_,
        const bool auto_close_ = false);

    String getName() const override { return "MySQL"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    Block readImpl() override;

    mysqlxx::PoolWithFailover::Entry entry;
    mysqlxx::Query query;
    mysqlxx::UseQueryResult result;
    const UInt64 max_block_size;
    const bool auto_close;
    ExternalResultDescription description;
};

}
