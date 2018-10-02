#pragma once

#include <Poco/Data/SessionPool.h>
#include <Poco/URI.h>

#include <Common/ODBCBridgeHelper.h>

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/DictionaryStructure.h>

#include <IO/ConnectionTimeouts.h>


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


/// Allows loading dictionaries from a ODBC source
class ODBCDictionarySource final : public IDictionarySource
{
public:
    ODBCDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        const Block & sample_block, const Context & context);

    /// copy-constructor is provided in order to support cloneability
    ODBCDictionarySource(const ODBCDictionarySource & other);

    BlockInputStreamPtr loadAll() override;

    BlockInputStreamPtr loadUpdatedAll() override;

    BlockInputStreamPtr loadIds(const std::vector<UInt64> & ids) override;

    BlockInputStreamPtr loadKeys(
        const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override;

    bool supportsSelectiveLoad() const override;

    bool hasUpdateField() const override;

    DictionarySourcePtr clone() const override;

    std::string toString() const override;

private:
    std::string getUpdateFieldAndDate();

    // execute invalidate_query. expects single cell in result
    std::string doInvalidateQuery(const std::string & request) const;

    BlockInputStreamPtr loadBase(const std::string & query) const;

    Poco::Logger * log;

    std::chrono::time_point<std::chrono::system_clock> update_time;
    const DictionaryStructure dict_struct;
    const std::string db;
    const std::string table;
    const std::string where;
    const std::string update_field;
    Block sample_block;
    ExternalQueryBuilder query_builder;
    const std::string load_all_query;
    std::string invalidate_query;
    mutable std::string invalidate_query_response;

    ODBCBridgeHelper odbc_bridge_helper;
    Poco::URI bridge_url;
    ConnectionTimeouts timeouts;
    const Context & global_context;

};


}
