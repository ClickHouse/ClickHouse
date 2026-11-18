#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class MetadataStorageFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<MetadataStoragePtr(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages)>;

    static MetadataStorageFactory & instance();

    void registerMetadataStorageType(const std::string & metadata_type, Creator creator);

    MetadataStoragePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages,
        const std::string & compatibility_type_hint) const;

    static std::string getMetadataType(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const std::string & compatibility_type_hint = "");

    static std::string getCompatibilityMetadataTypeHint(
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages);

    void clearRegistry();

private:
    using Registry = std::unordered_map<String, Creator>;
    Registry registry;
};

}
