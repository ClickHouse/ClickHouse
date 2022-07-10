#pragma once

#include <IO/ConnectionTimeouts.h>
#include <Poco/Data/SessionPool.h>
#include <Poco/URI.h>
#include <BridgeHelper/XDBCBridgeHelper.h>
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include "IDictionarySource.h"


namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}

class Logger;
}


namespace DB
{
/// Allows loading dictionaries from a XDBC source via bridges
class XDBCDictionarySource final : public IDictionarySource, WithContext
{
public:

    struct Configuration
    {
        const std::string db;
        const std::string schema;
        const std::string table;
        const std::string query;
        const std::string where;
        const std::string invalidate_query;
        const std::string update_field;
        const UInt64 update_lag;
    };

    XDBCDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        const Block & sample_block_,
        ContextPtr context_,
        BridgeHelperPtr bridge);

    /// copy-constructor is provided in order to support cloneability
    XDBCDictionarySource(const XDBCDictionarySource & other);
    XDBCDictionarySource & operator=(const XDBCDictionarySource &) = delete;

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override;

    QueryPipeline loadIds(const std::vector<UInt64> & ids) override;

    QueryPipeline loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    std::string getUpdateFieldAndDate();

    // execute invalidate_query. expects single cell in result
    std::string doInvalidateQuery(const std::string & request) const;

    QueryPipeline loadFromQuery(const Poco::URI & url, const Block & required_sample_block, const std::string & query) const;

    Poco::Logger * log;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const Configuration configuration;
    Block sample_block;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    mutable std::string invalidate_query_response;

    BridgeHelperPtr bridge_helper;
    Poco::URI bridge_url;
    ConnectionTimeouts timeouts;
    Poco::Net::HTTPBasicCredentials credentials{};
};

}
