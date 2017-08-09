#pragma once

#include <Interpreters/Context.h>

#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>

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
    virtual Context & context() const = 0;

    virtual ~IServer() {}
};

}
