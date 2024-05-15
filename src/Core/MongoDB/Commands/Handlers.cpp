#include "Handlers.h"

namespace DB
{
namespace MongoDB
{

BSON::Document::Ptr handleIsMaster()
{
    // TODO make all settings configable
    BSON::Document::Ptr response = new BSON::Document();
    Poco::Timestamp current_time;
    current_time.update();
    BSON::Document::Ptr topology_version = new BSON::Document();
    (*topology_version).add("processId", BSON::ObjectId::Ptr(new BSON::ObjectId("661d3910ecbeac371e0f1d23"))).add("counter", 0);
    (*response)
        .add("helloOk", true)
        .add("ismaster", true)
        .add("isWritablePrimary", true)
        .add("topologyVersion", topology_version)
        .add("maxBsonObjectSize", 16777216)
        .add("maxMessageSizeBytes", 48000000)
        .add("maxWriteBatchSize", 100000)
        .add("localTime", current_time)
        .add("logicalSessionTimeoutMinutes", 30)
        .add("connectionId", 228)
        .add("minWireVersion", 0)
        .add("maxWireVersion", 17)
        .add("readOnly", false)
        .add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleBuildInfo()
{
    // FIXME normal parameters
    BSON::Document::Ptr response = new BSON::Document();
    BSON::Array::Ptr empty_array = new BSON::Array();
    BSON::Array::Ptr version_array = new BSON::Array();
    (*version_array).add(6);
    (*version_array).add(0);
    (*version_array).add(10);
    (*version_array).add(0);
    BSON::Document::Ptr openssl = new BSON::Document();
    (*openssl).add("running", "Apple Secure Transport");
    BSON::Array::Ptr storage_engines = new BSON::Array();
    (*storage_engines).add("devnull");
    (*storage_engines).add("ephemeralForTest");
    (*storage_engines).add("wiredTiger");
    (*response)
        .add("version", "6.0.10")
        .add("gitVersion", "8e4b5670df9b9fe814e57cb5f3f8ee9407237b5a")
        .add("modules", empty_array)
        .add("allocator", "system")
        .add("javascriptEngine", "mozjs")
        .add("sysInfo", "deprecated")
        .add("versionArray", version_array)
        .add("openssl", openssl)
        .add("bits", 64)
        .add("debug", false)
        .add("maxBsonObjectSize", 16777216)
        .add("storageEngines", storage_engines)
        .add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleGetParameter(Command::Ptr command)
{
    auto element_names = command->getExtra()->elementNames();
    BSON::Document::Ptr doc = new BSON::Document();
    // FIXME make it map like, not if like
    for (const auto & parameter_name : element_names)
    {
        if (parameter_name == "featureCompatibilityVersion")
        {
            BSON::Document::Ptr version = new BSON::Document();
            version->add("version", "7.0");
            doc->add("featureCompatibilityVersion", version);
        }
    }
    doc->add("ok", 1.0);
    return doc;
}

BSON::Document::Ptr handlePing()
{
    BSON::Document::Ptr response = new BSON::Document();
    (*response).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleGetLog(Command::Ptr command)
{
    LOG_DEBUG(getLogger("handleGetLog"), "Not yet implemented {}", command->getName());
    BSON::Document::Ptr response = new BSON::Document();
    (*response).add("totalLinesWritten", 0).add("log", BSON::Array::Ptr(new BSON::Array())).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleUnknownCommand(Command::Ptr command)
{
    BSON::Document::Ptr response = new BSON::Document();
    (*response)
        .add("ok", 0.0)
        .add("errmsg", fmt::format("no such command: '{}'", command->getName()))
        .add("code", 59)
        .add("codeName", "CommandNotFound");
    return response;
}

BSON::Document::Ptr handleAtlasCLI(Command::Ptr command)
{
    LOG_DEBUG(getLogger("handleGetLog"), "Not yet implemented {}", command->getName());
    BSON::Document::Ptr response = new BSON::Document();
    BSON::Document::Ptr cursor = new BSON::Document();
    (*cursor).add("firstBatch", BSON::Array::Ptr(new BSON::Array())).add("id", 0).add("ns", "admin.atlascli");
    (*response).add("cursor", cursor).add("ok", 1.0);
    return response;
}

BSON::Document::Ptr handleError(const std::string & err_what)
{
    BSON::Document::Ptr response = new BSON::Document();
    (*response).add("ok", 0.0).add("errmsg", fmt::format("Caught Error: '{}'", err_what)).add("code", 1).add("codeName", "SomeError");
    return response;
}

}
} // namespace DB::MongoDB
