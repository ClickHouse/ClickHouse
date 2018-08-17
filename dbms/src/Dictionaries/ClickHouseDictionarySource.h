#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <memory>


namespace DB
{

/** Allows loading dictionaries from local or remote ClickHouse instance
  *    @todo use ConnectionPoolWithFailover
  *    @todo invent a way to keep track of source modifications
  */
class ClickHouseDictionarySource final : public IDictionarySource
{
public:
    ClickHouseDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block, Context & context);

    /// copy-constructor is provided in order to support cloneability
    ClickHouseDictionarySource(const ClickHouseDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override { return true; }
    bool supportsSelectiveLoad() const override { return true; }

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override { return std::make_unique<ClickHouseDictionarySource>(*this); }

    std::string toString() const override;

private:
    std::string getUpdateFieldAndDate();

    BlockInputStreamPtr createStreamForSelectiveLoad(const std::string & query);

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const std::string host;
    const UInt16 port;
    const bool secure;
    const std::string user;
    const std::string password;
    const std::string db;
    const std::string table;
    const std::string where;
    const std::string update_field;
    ExternalQueryBuilder query_builder;
    Block sample_block;
    Context & context;
    const bool is_local;
    ConnectionPoolWithFailoverPtr pool;
    const std::string load_all_query;
};

}
