#include <cstddef>
#include <string>
#include <Interpreters/Context.h>
#include <pybind11/pybind11.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <Client/LocalConnection.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/Config/ConfigProcessor.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

DB::ConfigurationPtr setupUsers();

const ContextHolder & getContext()
{
    static ContextHolder holder;
    holder.context->setUsersConfig(setupUsers());
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


namespace py = pybind11;

std::string hello()
{
    std::cout << __FILE__ << ":" << __LINE__ << "\n";

    const auto & ctx = getContext();

    DB::LocalConnection lc(ctx.context);
    std::string res;
    if (lc.checkConnected())
    {
        res += "yes";
    }
    else
    {
        res += "no";
    }

    DB::ConnectionTimeouts ts;
    lc.sendQuery(ts, "SELECT 1 as x", "", DB::QueryProcessingStage::Complete, nullptr, nullptr, false);
    //...
    return res;
    // return toString(DB::ServerUUID::get());
}

PYBIND11_MODULE(mylib, m) {
    m.doc() = "pybind11 mylib plugin";
    m.def("hello", &hello, "hello");
    // m.def("executeMultiQuery", &DB::LocalServer::main, "A function that adds two numbers");
}
