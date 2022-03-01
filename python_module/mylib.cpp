#include <cstddef>
#include <string>
#include <Core/Protocol.h>
#include <Interpreters/Context.h>
#include <fmt/format.h>
#include <pybind11/pybind11.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <Client/LocalConnection.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Databases/DatabaseMemory.h>
#include <fmt/format.h>

using namespace DB;

DB::ConfigurationPtr setupUsers();

static DatabasePtr createMemoryDatabaseIfNotExists(ContextPtr context, const String & database_name)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(database_name, context);
        DatabaseCatalog::instance().attachDatabase(database_name, system_database);
    }
    return system_database;
}


const ContextHolder & getContext()
{
    static ContextHolder holder;
    holder.context->setUsersConfig(setupUsers());

    DatabaseCatalog::instance().attachDatabase("default", std::make_shared<DatabaseMemory>("default", holder.context));
    holder.context->setCurrentDatabase("default");

    attachSystemTablesLocal(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::SYSTEM_DATABASE));
    attachInformationSchema(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::INFORMATION_SCHEMA));
    attachInformationSchema(holder.context, *createMemoryDatabaseIfNotExists(holder.context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));


    return holder;
}


static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


DB::ConfigurationPtr setupUsers()
{
    static const char * minimal_default_user_xml =
        "<clickhouse>"
        "    <profiles>"
        "        <default></default>"
        "    </profiles>"
        "    <users>"
        "        <default>"
        "            <password></password>"
        "            <networks>"
        "                <ip>::/0</ip>"
        "            </networks>"
        "            <profile>default</profile>"
        "            <quota>default</quota>"
        "        </default>"
        "    </users>"
        "    <quotas>"
        "        <default></default>"
        "    </quotas>"
        "</clickhouse>";


    return getConfigurationFromXMLString(minimal_default_user_xml);
}


bool readResult(DB::LocalConnection & lc, std::vector<std::string> & res)
{
    while (true)
    {
        auto p = lc.receivePacket();
        switch (p.type)
        {
            case Protocol::Server::EndOfStream:
                return true;
            case Protocol::Server::Data:
                std::cout << "Block sturcutre: " << p.block.dumpStructure() << "\n";
                if (p.block.has("x"))
                {
                    const auto & col = p.block.getByName("x");
                    for (size_t i = 0; i < col.column->size(); ++i)
                    {
                        Field f;
                        col.column->get(i, f);
                        res.push_back(f.dump());
                    }
                }
                break;
            case Protocol::Server::Exception:
                std::cout <<  __FILE__ << ":" << __LINE__ << (p.exception ? p.exception->what() : "???") << "\n";
                return false;
            default:
                std::cout <<  __FILE__ << ":" << __LINE__ << "packet type " << p.type << "\n";
                return false;
        }
    }
    __builtin_unreachable();
}


std::string hello(std::string query)
{
    /// TODO: initialization should be done once
    std::cout << __FILE__ << ":" << __LINE__ << "\n";

    const auto & ctx = getContext();

    DB::LocalConnection lc(ctx.context);

    if (!lc.checkConnected())
    {
        return "Error: cont connected";
    }

    DB::ConnectionTimeouts ts;
    lc.sendQuery(ts, query, "", DB::QueryProcessingStage::Complete, nullptr, nullptr, false);

    while (true)
    {
        /// TODO how to do `poll` and `receivePacket` ?
        /// Should we call one after another or wait unil poll finishes ad then call receivePacket?
        if (lc.poll(0))
            break;
        std::cout <<  __FILE__ << ":" << __LINE__ << "\n";
    }

    std::vector<std::string> res;
    readResult(lc, res);

    return fmt::format("{}", fmt::join(res, ", "));
}


PYBIND11_MODULE(mylib, m) {
    m.doc() = "pybind11 mylib plugin";
    m.def("hello", &hello, "hello");
    // m.def("executeMultiQuery", &DB::LocalServer::main, "A function that adds two numbers");
}
