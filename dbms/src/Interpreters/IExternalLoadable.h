#pragma once
#include <string>
#include <memory>
#include <Core/Types.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{

/// Min and max lifetimes for a loadable object or it's entry
struct ExternalLoadableLifetime final
{
    UInt64 min_sec;
    UInt64 max_sec;

    ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};

class IExternalLoadable : public std::enable_shared_from_this<IExternalLoadable>
{
public:
    virtual ~IExternalLoadable() = default;

    virtual const ExternalLoadableLifetime & getLifetime() const = 0;

    virtual std::string getName() const = 0;

    virtual bool supportUpdates() const = 0;

    virtual bool isModified() const = 0;

    virtual std::unique_ptr<IExternalLoadable> cloneObject() const = 0;

    virtual std::exception_ptr getCreationException() const = 0;
};

}
