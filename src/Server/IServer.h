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
    [[nodiscard]] virtual Poco::Util::LayeredConfiguration & config() const = 0;

    /// Returns the application's logger.
    [[nodiscard]] virtual Poco::Logger & logger() const = 0;

    /// Returns global application's context.
    [[nodiscard]] virtual ContextMutablePtr context() const = 0;

    /// Returns true if shutdown signaled.
    [[nodiscard]] virtual bool isCancelled() const = 0;

    virtual ~IServer() = default;
};

}
