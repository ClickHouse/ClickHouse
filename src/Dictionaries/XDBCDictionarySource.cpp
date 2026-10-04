#include <Dictionaries/XDBCDictionarySource.h>

#include <Columns/ColumnString.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Common/escapeForFileName.h>
#include <Core/ServerSettings.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Formats/IInputFormat.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsBool odbc_bridge_use_connection_pooling;

    /// Cloud only
    extern const SettingsBool cloud_mode;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    ExternalQueryBuilder makeExternalQueryBuilder(const DictionaryStructure & dict_struct_,
                                                  const std::string & db_,
                                                  const std::string & schema_,
                                                  const std::string & table_,
                                                  const std::string & query_,
                                                  const std::string & where_,
                                                  IXDBCBridgeHelper & bridge_)
    {
        QualifiedTableName qualified_name{schema_, table_};

        if (bridge_.isSchemaAllowed())
        {
            if (qualified_name.database.empty())
                qualified_name = QualifiedTableName::parseFromString(qualified_name.table);
        }
        else
        {
            if (!qualified_name.database.empty())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Dictionary source specifies a schema but schema is not supported by {}-driver",
                    bridge_.getName());
        }

        return {dict_struct_, db_, qualified_name.database, qualified_name.table, query_, where_, bridge_.getIdentifierQuotingStyle()};
    }
}

static const UInt64 max_block_size = 8192;


XDBCDictionarySource::XDBCDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    const Block & sample_block_,
    ContextPtr context_,
    const BridgeHelperPtr bridge_)
    : WithContext(context_->getGlobalContext())
    , log(getLogger(bridge_->getName() + "DictionarySource"))
    , update_time(std::chrono::system_clock::from_time_t(0))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.db, configuration.schema, configuration.table, configuration.query, configuration.where, *bridge_))
    , load_all_query(query_builder.composeLoadAllQuery())
    , bridge_helper(bridge_)
    , bridge_url(bridge_helper->getMainURI())
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context_->getSettingsRef(), context_->getServerSettings()))
{
    auto url_params = bridge_helper->getURLParams(max_block_size);
    for (const auto & [name, value] : url_params)
        bridge_url.addQueryParameter(name, value);
}

/// copy-constructor is provided in order to support cloneability
XDBCDictionarySource::XDBCDictionarySource(const XDBCDictionarySource & other)
    : WithContext(other.getContext())
    , log(getLogger(other.bridge_helper->getName() + "DictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , query_builder(other.query_builder)
    , load_all_query(other.load_all_query)
    , invalidate_query_response(other.invalidate_query_response)
    , bridge_helper(other.bridge_helper)
    , bridge_url(other.bridge_url)
    , timeouts(other.timeouts)
{
}


std::string XDBCDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }

    update_time = std::chrono::system_clock::now();
    return load_all_query;
}


BlockIO XDBCDictionarySource::loadAll()
{
    LOG_TRACE(log, fmt::runtime(load_all_query));
    BlockIO io;
    io.pipeline = loadFromQuery(bridge_url, sample_block, load_all_query);
    return io;
}


BlockIO XDBCDictionarySource::loadUpdatedAll()
{
    std::string load_query_update = getUpdateFieldAndDate();

    LOG_TRACE(log, fmt::runtime(load_query_update));
    BlockIO io;
    io.pipeline = loadFromQuery(bridge_url, sample_block, load_query_update);
    return io;
}


BlockIO XDBCDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    BlockIO io;
    io.pipeline = loadFromQuery(bridge_url, sample_block, query);
    return io;
}


BlockIO XDBCDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    BlockIO io;
    io.pipeline = loadFromQuery(bridge_url, sample_block, query);
    return io;
}


bool XDBCDictionarySource::supportsSelectiveLoad() const
{
    return true;
}


bool XDBCDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}


DictionarySourcePtr XDBCDictionarySource::clone() const
{
    return std::make_shared<XDBCDictionarySource>(*this);
}


std::string XDBCDictionarySource::toString() const
{
    const auto & where = configuration.where;
    return bridge_helper->getName() + ": " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}


bool XDBCDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        return invalidate_query_response.updateAndCheckModified(response);
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
    auto url_params = bridge_helper->getURLParams(max_block_size);
    for (const auto & [name, value] : url_params)
        invalidate_url.addQueryParameter(name, value);

    QueryPipeline pipeline = loadFromQuery(invalidate_url, invalidate_sample_block, request);
    return readInvalidateQuery(pipeline);
}


QueryPipeline XDBCDictionarySource::loadFromQuery(const Poco::URI & uri, const Block & required_sample_block, const std::string & query) const
{
    bridge_helper->startBridgeSync();

    auto write_body_callback = [required_sample_block, query](std::ostream & os)
    {
        os << "sample_block=" << escapeForFileName(required_sample_block.getNamesAndTypesList().toString());
        os << "&";
        os << "query=" << escapeForFileName(query);
    };

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(timeouts)
                   .withOutCallback(std::move(write_body_callback))
                   .create(credentials);

    auto format = getContext()->getInputFormat(IXDBCBridgeHelper::DEFAULT_FORMAT, *buf, required_sample_block, max_block_size);
    format->addBuffer(std::move(buf));

    return QueryPipeline(std::move(format));
}

void registerDictionarySourceXDBC(DictionarySourceFactory & factory);
void registerDictionarySourceXDBC(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                   const DictionaryStructure & dict_struct,
                                   const Poco::Util::AbstractConfiguration & config,
                                   const std::string & config_prefix,
                                   Block & sample_block,
                                   ContextPtr global_context,
                                   const std::string & /* default_database */,
                                   bool /* check_config */) -> DictionarySourcePtr {

        if (global_context->getSettingsRef()[Setting::cloud_mode])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Dictionary source of type `odbc` is disabled");

        BridgeHelperPtr bridge = std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(
            global_context,
            global_context->getSettingsRef()[Setting::http_receive_timeout],
            config.getString(config_prefix + ".odbc.connection_string"),
            config.getBool(config_prefix + ".settings.odbc_bridge_use_connection_pooling",
            global_context->getSettingsRef()[Setting::odbc_bridge_use_connection_pooling]));

        std::string settings_config_prefix = config_prefix + ".odbc";

        XDBCDictionarySource::Configuration configuration
        {
            .db = config.getString(settings_config_prefix + ".db", ""),
            .schema = config.getString(settings_config_prefix + ".schema", ""),
            .table = config.getString(settings_config_prefix + ".table", ""),
            .query = config.getString(settings_config_prefix + ".query", ""),
            .where = config.getString(settings_config_prefix + ".where", ""),
            .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1)
        };

        return std::make_unique<XDBCDictionarySource>(dict_struct, configuration, sample_block, global_context, bridge);
    };
    factory.registerSource("odbc", create_table_source, Documentation{
        .description = R"DOCS_MD(
# ODBC dictionary source

You can use this method to connect any database that has an ODBC driver.

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
    <odbc>
        <db>DatabaseName</db>
        <table>ShemaName.TableName</table>
        <connection_string>DSN=some_parameters</connection_string>
        <invalidate_query>SQL_QUERY</invalidate_query>
        <query>SELECT id, value_1, value_2 FROM ShemaName.TableName</query>
    </odbc>
</source>
```

</Tab>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `db` | Name of the database. Omit it if the database name is set in the `<connection_string>` parameters. |
| `table` | Name of the table and schema if exists. |
| `connection_string` | Connection string. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](/reference/statements/create/dictionary/lifetime). |
| `background_reconnect` | Reconnect to replica in background if connection fails. Optional. |
| `query` | The custom query. Optional. |

<Note>
The `table` and `query` fields cannot be used together. And either one of the `table` or `query` fields must be declared.
</Note>

ClickHouse receives quoting symbols from ODBC-driver and quote all settings in queries to driver, so it's necessary to set table name accordingly to table name case in database.

If you have a problems with encodings when using Oracle, see the corresponding [FAQ](/resources/support-center/knowledge-base/integrations/oracle-odbc) item.

### Known Vulnerability of the ODBC Dictionary Functionality {#known-vulnerability-of-the-odbc-dictionary-functionality}

<Note>
When connecting to the database through the ODBC driver connection parameter `Servername` can be substituted. In this case values of `USERNAME` and `PASSWORD` from `odbc.ini` are sent to the remote server and can be compromised.
</Note>

**Example of insecure use**

Let's configure unixODBC for PostgreSQL. Content of `/etc/odbc.ini`:

```text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

If you then make a query such as

```sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBC driver will send values of `USERNAME` and `PASSWORD` from `odbc.ini` to `some-server.com`.

### Example of Connecting Postgresql {#example-of-connecting-postgresql}

Ubuntu OS.

Installing unixODBC and the ODBC driver for PostgreSQL:

```bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

Configuring `/etc/odbc.ini` (or `~/.odbc.ini` if you signed in under a user that runs ClickHouse):

```text
    [DEFAULT]
    Driver = myconnection

    [myconnection]
    Description         = PostgreSQL connection to my_db
    Driver              = PostgreSQL Unicode
    Database            = my_db
    Servername          = 127.0.0.1
    UserName            = username
    Password            = password
    Port                = 5432
    Protocol            = 9.3
    ReadOnly            = No
    RowVersioning       = No
    ShowSystemTables    = No
    ConnSettings        =
```

The dictionary configuration in ClickHouse:

<Tabs>
<Tab title="DDL">

```sql
CREATE DICTIONARY table_name (
    id UInt64,
    some_column UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360)
```

</Tab>
<Tab title="Configuration file">

```xml
<clickhouse>
    <dictionary>
        <name>table_name</name>
        <source>
            <odbc>
                <!-- You can specify the following parameters in connection_string: -->
                <!-- DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db -->
                <connection_string>DSN=myconnection</connection_string>
                <table>postgresql_table</table>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>some_column</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

</Tab>
</Tabs>
<br/>

You may need to edit `odbc.ini` to specify the full path to the library with the driver `DRIVER=/usr/local/lib/psqlodbcw.so`.

### Example of Connecting MS SQL Server {#example-of-connecting-ms-sql-server}

Ubuntu OS.

Installing the ODBC driver for connecting to MS SQL:

```bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

Configuring the driver:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # test TDS connection
    $ sqsh -S MSSQL -D database -U user -P password

    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # if you signed in under a user that runs ClickHouse

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433

    # (optional) test ODBC connection (to use isql-tool install the [unixodbc](https://packages.debian.org/sid/unixodbc)-package)
    $ isql -v MSSQL "user" "password"
```

Remarks:
- to determine the earliest TDS version that is supported by a particular SQL Server version, refer to the product documentation or look at [MS-TDS Product Behavior](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)

Configuring the dictionary in ClickHouse:

<Tabs>
<Tab title="DDL">

```sql
CREATE DICTIONARY test (
    k UInt64,
    s String DEFAULT ''
)
PRIMARY KEY k
SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 360)
```

</Tab>
<Tab title="Configuration file">

```xml
<clickhouse>
    <dictionary>
        <name>test</name>
        <source>
            <odbc>
                <table>dict</table>
                <connection_string>DSN=MSSQL;UID=test;PWD=test</connection_string>
            </odbc>
        </source>

        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>

        <layout>
            <flat />
        </layout>

        <structure>
            <id>
                <name>k</name>
            </id>
            <attribute>
                <name>s</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

</Tab>
</Tabs>
)DOCS_MD",
        .syntax = "SOURCE(ODBC(connection_string 'DSN=...' table 'table'))",
        .related = {"jdbc"}});
}


void registerDictionarySourceJDBC(DictionarySourceFactory & factory);
void registerDictionarySourceJDBC(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & /* dict_struct */,
                                 const Poco::Util::AbstractConfiguration & /* config */,
                                 const std::string & /* config_prefix */,
                                 Block & /* sample_block */,
                                 ContextPtr /* global_context */,
                                 const std::string & /* default_database */,
                                 bool /* created_from_ddl */) -> DictionarySourcePtr {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `jdbc` is disabled until consistent support for nullable fields.");
    };
    factory.registerSource("jdbc", create_table_source, Documentation{
        .description = "Reads dictionary data from an external database over JDBC, via the `clickhouse-jdbc-bridge` program. Currently disabled, pending consistent support for nullable fields.",
        .syntax = "SOURCE(JDBC(datasource '...' table 'table'))",
        .related = {"odbc"}});
}

}
