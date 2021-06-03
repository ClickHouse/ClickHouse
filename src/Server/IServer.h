#pragma once

#include <Interpreters/Context_fwd.h>

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

class IServer
{
public:
    /// Returns the application's configuration.
    virtual Poco::Util::LayeredConfiguration & config() const = 0;

    /// Returns the application's logger.
    virtual Poco::Logger & logger() const = 0;

    /// Returns global application's context.
    virtual ContextPtr context() const = 0;

    /// Returns true if shutdown signaled.
    virtual bool isCancelled() const = 0;

    virtual ~IServer() = default;
};

}
