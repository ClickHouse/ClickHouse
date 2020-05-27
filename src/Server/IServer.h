#pragma once

namespace Poco
{

namespace Util
{
class LayeredConfiguration;
}
class Logger;

}


namespace DB
{

class Context;

class IServer
{
public:
    /// Returns the application's configuration.
    virtual Poco::Util::LayeredConfiguration & config() const = 0;

    /// Returns the application's logger.
    virtual Poco::Logger & logger() const = 0;

    /// Returns global application's context.
    virtual Context & context() const = 0;

    /// Returns true if shutdown signaled.
    virtual bool isCancelled() const = 0;

    virtual ~IServer() {}
};

}
