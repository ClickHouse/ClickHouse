#include <StatusPage.h>

#if USE_SILK

#include <Router.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>

#include <sstream>


namespace DB::Proxy
{

namespace
{

Poco::JSON::Object::Ptr describeBackend(const Backend & backend)
{
    auto object = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    object->set("name", backend.name());
    object->set("host", backend.config().host);
    object->set("alive", backend.isAlive());
    object->set("weight", backend.config().weight);
    object->set("active_connections", backend.activeConnections());
    object->set("total_connections", backend.totalConnections());
    object->set("total_errors", backend.totalErrors());
    object->set("connections_per_second", backend.connectionsPerSecond());
    object->set("connect_latency_ms", backend.connectLatencyMs());
    object->set("check_latency_ms", backend.checkLatencyMs());
    object->set("bytes_from_client", backend.bytesFromClient());
    object->set("bytes_to_client", backend.bytesToClient());
    if (backend.cpuUsage() >= 0)
        object->set("cpu_cores", backend.cpuUsage());
    if (backend.memoryUsage() >= 0)
        object->set("memory_bytes", backend.memoryUsage());
    return object;
}

void describePool(Poco::JSON::Array & pools, const BackendPool & pool)
{
    auto object = Poco::JSON::Object::Ptr(new Poco::JSON::Object);
    object->set("name", pool.name());
    object->set("load_balancing", String(pool.loadBalancingName()));

    auto backends = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    for (const auto & backend : pool.backends())
        backends->add(describeBackend(*backend));
    object->set("backends", backends);

    pools.add(object);
}

}

String buildStatusJSON(const Router & router)
{
    Poco::JSON::Object root;

    auto pools = Poco::JSON::Array::Ptr(new Poco::JSON::Array);
    for (const auto & [_, pool] : router.staticPools())
        describePool(*pools, *pool);
    for (const auto & pool : router.dynamicPoolsSnapshot())
        describePool(*pools, *pool);
    root.set("pools", pools);

    std::ostringstream out;    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root.stringify(out, 2);
    return out.str();
}

}

#endif
