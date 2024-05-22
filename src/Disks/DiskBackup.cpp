#include "DiskBackup.h"

#include <Disks/DiskFactory.h>
#include "Disks/IDisk.h"
#include <Disks/ObjectStorages/MetadataStorageFactory.h>


namespace DB
{

DiskBackup::DiskBackup(const String & name_, DiskPtr delegate_, MetadataStoragePtr metadata_) : IDisk(name_), delegate(delegate_), metadata(metadata_)
{

}

DiskBackup::DiskBackup(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_) : IDisk(name_)
{
    String disk_delegate_name = config_.getString(config_prefix_ + ".disk_delegate");
    String metadata_name = config_.getString(config_prefix_ + ".metadata");

    delegate = map_.at(disk_delegate_name);
    metadata = MetadataStorageFactory::instance().create(metadata_name, config_, config_prefix_, nullptr, "");
}

bool DiskBackup::exists(const String & path) const
{
    return metadata->exists(fs::path(getPath()) / path);
}

bool DiskBackup::isFile(const String & path) const
{
    return metadata->isFile(fs::path(getPath()) / path);
}

bool DiskBackup::isDirectory(const String & path) const
{
    return metadata->isDirectory(fs::path(getPath()) / path);
}

size_t DiskBackup::getFileSize(const String & path) const
{
    return metadata->getFileSize(fs::path(getPath()) / path);
}

DirectoryIteratorPtr DiskBackup::iterateDirectory(const String & path) const
{
    return metadata->iterateDirectory(fs::path(getPath()) / path);
}

void DiskBackup::listFiles(const String & path, std::vector<String> & file_names) const
{
    file_names = metadata->listDirectory(fs::path(getPath()) / path);
}

void registerDiskBackup(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool, bool) -> DiskPtr
    {
        std::shared_ptr<IDisk> disk = std::make_shared<DiskBackup>(name, config, config_prefix, map);

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        disk->startup(context, skip_access_check);
        return disk;
    };
    factory.registerDiskType("backup", creator);
}

}

