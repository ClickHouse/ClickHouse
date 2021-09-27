#pragma once

#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{

class CassandraBlockInputStream final : public IBlockInputStream
{
public:
    CassandraBlockInputStream(
            const CassSessionShared & session_,
            const String & query_str,
            const Block & sample_block,
            size_t max_block_size);

    String getName() const override { return "Cassandra"; }

    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

    void readPrefix() override;

private:
    using ValueType = ExternalResultDescription::ValueType;

    Block readImpl() override;
    static void insertValue(IColumn & column, ValueType type, const CassValue * cass_value);
    void assertTypes(const CassResultPtr & result);

    CassSessionShared session;
    CassStatementPtr statement;
    CassFuturePtr result_future;
    const size_t max_block_size;
    ExternalResultDescription description;
    cass_bool_t has_more_pages;
    bool assert_types = true;
};

}

#endif
