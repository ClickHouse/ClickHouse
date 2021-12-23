#pragma once

#include <Dictionaries/CassandraHelpers.h>

#if USE_CASSANDRA
#include <Core/Block.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Core/ExternalResultDescription.h>


namespace DB
{

class CassandraSource final : public SourceWithProgress
{
public:
    CassandraSource(
            const CassSessionShared & session_,
            const String & query_str,
            const Block & sample_block,
            size_t max_block_size);

    String getName() const override { return "Cassandra"; }

private:
    using ValueType = ExternalResultDescription::ValueType;

    Chunk generate() override;
    static void insertValue(IColumn & column, ValueType type, const CassValue * cass_value);
    void assertTypes(const CassResultPtr & result);

    CassSessionShared session;
    CassStatementPtr statement;
    CassFuturePtr result_future;
    const size_t max_block_size;
    ExternalResultDescription description;
    cass_bool_t has_more_pages;
    bool assert_types = true;
    bool is_initialized = false;
};

}

#endif
