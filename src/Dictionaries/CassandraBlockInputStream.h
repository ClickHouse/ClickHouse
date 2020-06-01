#pragma once

#include <Dictionaries/CassandraHelpers.h>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{



/// Allows processing results of a Cassandra query as a sequence of Blocks, simplifies chaining
    class CassandraBlockInputStream final : public IBlockInputStream
    {
    public:
        CassandraBlockInputStream(
                const CassClusterPtr & cluster,
                const String & query_str,
                const Block & sample_block,
                const size_t max_block_size);

        String getName() const override { return "Cassandra"; }

        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        Block readImpl() override;

        CassSessionPtr session;
        CassStatementPtr statement;
        const size_t max_block_size;
        ExternalResultDescription description;
        cass_bool_t has_more_pages;
    };

}
