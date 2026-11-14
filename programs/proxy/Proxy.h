#pragma once

#include "config.h"

#include <Daemon/BaseDaemon.h>

#include <memory>

namespace DB
{

namespace Proxy
{
class ProxyServer;
}

/// A lightweight standalone application that proxies end-user ClickHouse protocols (HTTP, native,
/// MySQL, PostgreSQL, and opaque TLS/TCP streams) to backend servers chosen by a configurable
/// routing table. It runs on a cooperative fiber scheduler to sustain many connections cheaply.
class ProxyApplication : public BaseDaemon
{
public:
    using ServerApplication::run;

protected:
    void initialize(Application & self) override;
    void uninitialize() override;
    int main(const std::vector<std::string> & args) override;
    std::string getDefaultConfigFileName() const override;
    bool allowTextLog() const override;

private:
#if USE_SILK
    /// Guarded because `ProxyServer` is only defined when silk is available, and the implicit
    /// destructor of `ProxyApplication` needs the complete type to destroy the `unique_ptr`.
    std::unique_ptr<Proxy::ProxyServer> server;
#endif
};

}
