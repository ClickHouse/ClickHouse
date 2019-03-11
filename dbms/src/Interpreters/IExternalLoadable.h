#pragma once

#include <chrono>
#include <string>
#include <memory>
#include <boost/noncopyable.hpp>
#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/// Min and max lifetimes for a loadable object or it's entry
struct ExternalLoadableLifetime final
{
    UInt64 min_sec = 0;
    UInt64 max_sec = 0;

    ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
    explicit ExternalLoadableLifetime(const ASTKeyValueFunction * lifetime);
};


/// Basic interface for external loadable objects. Is used in ExternalLoader.
class IExternalLoadable : public std::enable_shared_from_this<IExternalLoadable>, private boost::noncopyable
{
public:
    using LoadablePtr = std::shared_ptr<IExternalLoadable>;

    virtual ~IExternalLoadable() = default;

    virtual const ExternalLoadableLifetime & getLifetime() const = 0;

    virtual std::string getName() const = 0;
    /// True if object can be updated when lifetime exceeded.
    virtual bool supportUpdates() const = 0;
    /// If lifetime exceeded and isModified(), ExternalLoader replace current object with the result of clone().
    virtual bool isModified() const = 0;
    /// Returns new object with the same configuration. Is used to update modified object when lifetime exceeded.
    virtual LoadablePtr clone() const = 0;

    virtual std::chrono::time_point<std::chrono::system_clock> getCreationTime() const = 0;

    virtual std::exception_ptr getCreationException() const = 0;
};

}
