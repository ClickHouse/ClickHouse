#include "XDBCDictionarySource.h"

#include <Columns/ColumnString.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/XDBCBridgeHelper.h>
#include <common/LocalDateTime.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "readInvalidateQuery.h"

#include <Common/config.h>
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
#    include <Poco/Data/ODBC/Connector.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    class XDBCBridgeBlockInputStream : public IBlockInputStream
    {
    public:
        XDBCBridgeBlockInputStream(
            const Poco::URI & uri,
            std::function<void(std::ostream &)> callback,
            const Block & sample_block,
            const Context & context,
            UInt64 max_block_size,
            const ConnectionTimeouts & timeouts,
            const String name)
            : name(name)
        {
            read_buf = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, callback, timeouts);
            reader
                = FormatFactory::instance().getInput(IXDBCBridgeHelper::DEFAULT_FORMAT, *read_buf, sample_block, context, max_block_size);
        }

        Block getHeader() const override { return reader->getHeader(); }

        String getName() const override { return name; }

    private:
        Block readImpl() override { return reader->read(); }

        String name;
        std::unique_ptr<ReadWriteBufferFromHTTP> read_buf;
        BlockInputStreamPtr reader;
    };
}

static const UInt64 max_block_size = 8192;


XDBCDictionarySource::XDBCDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::string & config_prefix_,
    const Block & sample_block_,
    const Context & context_,
    const BridgeHelperPtr bridge_)
    : log(&Logger::get(bridge_->getName() + "DictionarySource"))
    , update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , db{config_.getString(config_prefix_ + ".db", "")}
    , table{config_.getString(config_prefix_ + ".table")}
    , where{config_.getString(config_prefix_ + ".where", "")}
    , update_field{config_.getString(config_prefix_ + ".update_field", "")}
    , sample_block{sample_block_}
    , query_builder{dict_struct, db, table, where, bridge_->getIdentifierQuotingStyle()}
    , load_all_query{query_builder.composeLoadAllQuery()}
    , invalidate_query{config_.getString(config_prefix_ + ".invalidate_query", "")}
    , bridge_helper{bridge_}
    , timeouts{ConnectionTimeouts::getHTTPTimeouts(context_)}
    , global_context(context_)
{
    bridge_url = bridge_helper->getMainURI();

    auto url_params = bridge_helper->getURLParams(sample_block_.getNamesAndTypesList().toString(), max_block_size);
    for (const auto & [name, value] : url_params)
        bridge_url.addQueryParameter(name, value);
}

/// copy-constructor is provided in order to support cloneability
XDBCDictionarySource::XDBCDictionarySource(const XDBCDictionarySource & other)
    : log(&Logger::get(other.bridge_helper->getName() + "DictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , db{other.db}
    , table{other.table}
    , where{other.where}
    , update_field{other.update_field}
    , sample_block{other.sample_block}
    , query_builder{dict_struct, db, table, where, other.bridge_helper->getIdentifierQuotingStyle()}
    , load_all_query{other.load_all_query}
    , invalidate_query{other.invalidate_query}
    , invalidate_query_response{other.invalidate_query_response}
    , bridge_helper{other.bridge_helper}
    , bridge_url{other.bridge_url}
    , timeouts{other.timeouts}
    , global_context{other.global_context}
{
}

std::string XDBCDictionarySource::getUpdateFieldAndDate()
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

BlockInputStreamPtr XDBCDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return loadBase(load_all_query);
}

BlockInputStreamPtr XDBCDictionarySource::loadUpdatedAll()
{
    std::string load_query_update = getUpdateFieldAndDate();

    LOG_TRACE(log, load_query_update);
    return loadBase(load_query_update);
}

BlockInputStreamPtr XDBCDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadBase(query);
}

BlockInputStreamPtr XDBCDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadBase(query);
}

bool XDBCDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool XDBCDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

DictionarySourcePtr XDBCDictionarySource::clone() const
{
    return std::make_unique<XDBCDictionarySource>(*this);
}

std::string XDBCDictionarySource::toString() const
{
    return bridge_helper->getName() + ": " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

bool XDBCDictionarySource::isModified() const
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


std::string XDBCDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));

    bridge_helper->startBridgeSync();

    auto invalidate_url = bridge_helper->getMainURI();
    auto url_params = bridge_helper->getURLParams(invalidate_sample_block.getNamesAndTypesList().toString(), max_block_size);
    for (const auto & [name, value] : url_params)
        invalidate_url.addQueryParameter(name, value);

    XDBCBridgeBlockInputStream stream(
        invalidate_url,
        [request](std::ostream & os) { os << "query=" << request; },
        invalidate_sample_block,
        global_context,
        max_block_size,
        timeouts,
        bridge_helper->getName() + "BlockInputStream");

    return readInvalidateQuery(stream);
}

BlockInputStreamPtr XDBCDictionarySource::loadBase(const std::string & query) const
{
    bridge_helper->startBridgeSync();
    return std::make_shared<XDBCBridgeBlockInputStream>(
        bridge_url,
        [query](std::ostream & os) { os << "query=" << query; },
        sample_block,
        global_context,
        max_block_size,
        timeouts,
        bridge_helper->getName() + "BlockInputStream");
}

void registerDictionarySourceXDBC(DictionarySourceFactory & factory)
{
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
    Poco::Data::ODBC::Connector::registerConnector();
#endif

    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 Context & context) -> DictionarySourcePtr {
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
        BridgeHelperPtr bridge = std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(
            context, context.getSettings().http_receive_timeout, config.getString(config_prefix + ".odbc.connection_string"));
        return std::make_unique<XDBCDictionarySource>(dict_struct, config, config_prefix + ".odbc", sample_block, context, bridge);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        (void)context;
        throw Exception{"Dictionary source of type `odbc` is disabled because poco library was built without ODBC support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    factory.registerSource("odbc", createTableSource);
}

void registerDictionarySourceJDBC(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & /* dict_struct */,
                                 const Poco::Util::AbstractConfiguration & /* config */,
                                 const std::string & /* config_prefix */,
                                 Block & /* sample_block */,
                                 const Context & /* context */) -> DictionarySourcePtr {
        throw Exception{"Dictionary source of type `jdbc` is disabled until consistent support for nullable fields.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
        //        BridgeHelperPtr bridge = std::make_shared<XDBCBridgeHelper<JDBCBridgeMixin>>(config, context.getSettings().http_receive_timeout, config.getString(config_prefix + ".connection_string"));
        //        return std::make_unique<XDBCDictionarySource>(dict_struct, config, config_prefix + ".jdbc", sample_block, context, bridge);
    };
    factory.registerSource("jdbc", createTableSource);
}


}
