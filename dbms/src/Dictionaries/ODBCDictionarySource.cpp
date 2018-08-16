#include <Dictionaries/ODBCDictionarySource.h>
#include <common/logger_useful.h>
#include <common/LocalDateTime.h>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPRequest.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace
{
    class ODBCBridgeBlockInputStream : public IProfilingBlockInputStream
    {
    public:
        ODBCBridgeBlockInputStream(const Poco::URI & uri,
            std::function<void(std::ostream &)> callback,
            const Block & sample_block,
            const Context & context,
            size_t max_block_size,
            const ConnectionTimeouts & timeouts)
        {
            read_buf = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, callback, timeouts);
            reader = FormatFactory::instance().getInput(ODBCBridgeHelper::DEFAULT_FORMAT, *read_buf, sample_block, context, max_block_size);
        }

        Block getHeader() const override
        {
            return reader->getHeader();
        }

        String getName() const override
        {
            return "ODBCBridgeBlockInputStream";
        }

    private:
        Block readImpl() override
        {
            return reader->read();
        }

        std::unique_ptr<ReadWriteBufferFromHTTP> read_buf;
        BlockInputStreamPtr reader;
    };
}

static const size_t max_block_size = 8192;


ODBCDictionarySource::ODBCDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const Block & sample_block, const Context & context)
    : log(&Logger::get("ODBCDictionarySource")),
    update_time{std::chrono::system_clock::from_time_t(0)},
    dict_struct{dict_struct_},
    db{config.getString(config_prefix + ".db", "")},
    table{config.getString(config_prefix + ".table")},
    where{config.getString(config_prefix + ".where", "")},
    update_field{config.getString(config_prefix + ".update_field", "")},
    sample_block{sample_block},
    query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::None},    /// NOTE Better to obtain quoting style via ODBC interface.
    load_all_query{query_builder.composeLoadAllQuery()},
    invalidate_query{config.getString(config_prefix + ".invalidate_query", "")},
    odbc_bridge_helper{config, context.getSettingsRef().http_receive_timeout.value, config.getString(config_prefix + ".connection_string")},
    timeouts{ConnectionTimeouts::getHTTPTimeouts(context.getSettingsRef())},
    global_context(context)
{
    const auto & global_config = context.getConfigRef();
    size_t bridge_port = global_config.getUInt("odbc_bridge.port", ODBCBridgeHelper::DEFAULT_PORT);
    std::string bridge_host = global_config.getString("odbc_bridge.host", ODBCBridgeHelper::DEFAULT_HOST);

    bridge_url.setHost(bridge_host);
    bridge_url.setPort(bridge_port);
    bridge_url.setScheme("http");

    auto url_params = odbc_bridge_helper.getURLParams(sample_block.getNamesAndTypesList().toString(), max_block_size);
    for (const auto & [name, value] : url_params)
        bridge_url.addQueryParameter(name, value);
}

/// copy-constructor is provided in order to support cloneability
ODBCDictionarySource::ODBCDictionarySource(const ODBCDictionarySource & other)
    : log(&Logger::get("ODBCDictionarySource")),
    update_time{other.update_time},
    dict_struct{other.dict_struct},
    db{other.db},
    table{other.table},
    where{other.where},
    update_field{other.update_field},
    sample_block{other.sample_block},
    query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::None},
    load_all_query{other.load_all_query},
    invalidate_query{other.invalidate_query},
    invalidate_query_response{other.invalidate_query_response},
    odbc_bridge_helper{other.odbc_bridge_helper},
    global_context{other.global_context}
{

}

std::string ODBCDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        auto tmp_time = update_time;
        update_time = std::chrono::system_clock::now();
        time_t hr_time = std::chrono::system_clock::to_time_t(tmp_time) - 1;
        std::string str_time = std::to_string(LocalDateTime(hr_time));
        return query_builder.composeUpdateQuery(update_field, str_time);
    }
    else
    {
        update_time = std::chrono::system_clock::now();
        std::string str_time("0000-00-00 00:00:00"); ///for initial load
        return query_builder.composeUpdateQuery(update_field, str_time);
    }
}

BlockInputStreamPtr ODBCDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return loadBase(load_all_query);
}

BlockInputStreamPtr ODBCDictionarySource::loadUpdatedAll()
{
    std::string load_query_update = getUpdateFieldAndDate();

    LOG_TRACE(log, load_query_update);
    return loadBase(load_query_update);
}

BlockInputStreamPtr ODBCDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadBase(query);
}

BlockInputStreamPtr ODBCDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadBase(query);
}

bool ODBCDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ODBCDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

DictionarySourcePtr ODBCDictionarySource::clone() const
{
    return std::make_unique<ODBCDictionarySource>(*this);
}

std::string ODBCDictionarySource::toString() const
{
    return "ODBC: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

bool ODBCDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
}


std::string ODBCDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    odbc_bridge_helper.startODBCBridgeSync();

    ODBCBridgeBlockInputStream stream(
        bridge_url,
        [request](std::ostream & os) { os << "query=" << request; },
        invalidate_sample_block,
        global_context,
        max_block_size,
        timeouts);

    return readInvalidateQuery(stream);
}

BlockInputStreamPtr ODBCDictionarySource::loadBase(const std::string & query) const
{
    odbc_bridge_helper.startODBCBridgeSync();
    return std::make_shared<ODBCBridgeBlockInputStream>(bridge_url,
        [query](std::ostream & os) { os << "query=" << query; },
        sample_block,
        global_context,
        max_block_size,
        timeouts);
}

}
