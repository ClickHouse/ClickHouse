#pragma once

#include "config.h"

#if USE_CASSANDRA

#include <Core/ExternalResultDescription.h>
#include <Dictionaries/CassandraClient.h>
#include <Processors/ISource.h>


namespace DB
{

/// Reads the result of a query from Cassandra page by page.
class CassandraSource final : public ISource
{
public:
    CassandraSource(
        Cassandra::ConnectionPoolPtr pool_,
        const String & query_,
        const SharedHeader & sample_block,
        size_t max_block_size_);

    String getName() const override { return "Cassandra"; }

private:
    using ValueType = ExternalResultDescription::ValueType;

    Chunk generate() override;
    void checkColumns(const Cassandra::ResultPage & page) const;
    static void insertValue(IColumn & column, ValueType type, std::string_view value);

    Cassandra::ConnectionPoolPtr pool;
    const String query;
    const size_t max_block_size;
    ExternalResultDescription description;

    String paging_state;
    bool is_finished = false;
};

}

#endif
