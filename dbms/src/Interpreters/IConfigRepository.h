#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/Timestamp.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <string>
#include <set>

namespace DB
{

/** Repository with configurations of user-defined objects (dictionaries, models).
  * Used by ExternalLoader.
  */
class IConfigRepository
{
public:
    using Files = std::set<std::string>;
    virtual Files list(const Poco::Util::AbstractConfiguration & config, const std::string & path_key) const = 0;

    virtual bool exists(const std::string & config_file) const = 0;

    virtual Poco::Timestamp getLastModificationTime(const std::string & config_file) const = 0;

    virtual Poco::AutoPtr<Poco::Util::AbstractConfiguration> load(const std::string & config_file, const std::string & preprocessed_dir = "") const = 0;

    virtual String getSource() const = 0;

    virtual ~IConfigRepository() = default;
};


class InternalConfigRepository : IConfigRepository
{
public:
    InternalConfigRepository(Context & context)
    {
        (void)context;
    }

    Files list(const Poco::Util::AbstractConfiguration & config, const String & path) const override
    {
        (void)config;
        (void)path;
        return {};
    }


    String getSource() const override
    {
        return "Memory";
    }
private:
    void loadConfigurations();
    void reloadConfigurations();


    using Object = int;
    std::unordered_map<String, Object> map;
};

}
