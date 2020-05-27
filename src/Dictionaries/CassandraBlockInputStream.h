#pragma once

#include <cassandra.h>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{

void cassandraCheck(CassError code);
void cassandraWaitAndCheck(CassFuture * future);


/// Allows processing results of a Cassandra query as a sequence of Blocks, simplifies chaining
    class CassandraBlockInputStream final : public IBlockInputStream
    {
    public:
        CassandraBlockInputStream(
                CassSession * session,
                const std::string & query_str,
                const Block & sample_block,
                const size_t max_block_size);
        ~CassandraBlockInputStream() override;

        String getName() const override { return "Cassandra"; }

        Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    private:
        Block readImpl() override;

        CassSession * session;
        CassStatement * statement;
        String query_str;
        const size_t max_block_size;
        ExternalResultDescription description;
        const CassResult * result = nullptr;
        cass_bool_t has_more_pages;
        CassIterator * iterator = nullptr;
    };

}
