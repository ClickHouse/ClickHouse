#pragma once

#include <string>
#include <memory>
#include <boost/noncopyable.hpp>
#include <pcg_random.hpp>
#include <Core/Types.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{

/// Min and max lifetimes for a loadable object or it's entry
struct ExternalLoadableLifetime
{
    UInt64 min_sec;
    UInt64 max_sec;

    ExternalLoadableLifetime(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};


/// Delay before trying to load again after error.
struct ExternalLoadableBackoff
{
    UInt64 backoff_initial_sec = 5;
    UInt64 backoff_max_sec = 10 * 60; /// 10 minutes

    /// Calculates time to try loading again after error.
    UInt64 calculateDuration(pcg64 & rnd_engine, size_t error_count = 1) const;
};


/// Basic interface for external loadable objects. Is used in ExternalLoader.
class IExternalLoadable : public std::enable_shared_from_this<IExternalLoadable>, private boost::noncopyable
{
public:
    virtual ~IExternalLoadable() = default;

    virtual const ExternalLoadableLifetime & getLifetime() const = 0;

    virtual std::string getName() const = 0;
    /// True if object can be updated when lifetime exceeded.
    virtual bool supportUpdates() const = 0;
    /// If lifetime exceeded and isModified(), ExternalLoader replace current object with the result of clone().
    virtual bool isModified() const = 0;
    /// Returns new object with the same configuration. Is used to update modified object when lifetime exceeded.
    virtual std::shared_ptr<const IExternalLoadable> clone() const = 0;
};

}
