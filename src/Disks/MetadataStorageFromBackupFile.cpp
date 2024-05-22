
#include <filesystem>

#include <Common/XMLUtils.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/StaticDirectoryIterator.h>
#include "Disks/DiskType.h"
#include "IO/ReadBufferFromFile.h"
#include "IO/ReadHelpers.h"
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Node.h>
#include <fstream>
#include <string>
#include <sstream>

#include <Poco/Util/XMLConfiguration.h>

#include "MetadataStorageFromBackupFile.h"


namespace DB
{

MetadataStorageFromBackupFile::MetadataStorageFromBackupFile(const String & path_to_backup_file)
{
    using namespace XMLUtils;

    ReadBufferFromFile buf(path_to_backup_file);
    String str;

    readStringUntilEOF(str, buf);
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> config = dom_parser.parseMemory(str.data(), str.size());
    const Poco::XML::Node * config_root = getRootNode(config);
    const auto * contents = config_root->getNodeByPath("contents");
    for (const Poco::XML::Node * child = contents->firstChild(); child; child = child->nextSibling())
    {
        if (child->nodeName() == "file")
        {
            const Poco::XML::Node * file_config = child;
            fs::path file_path = getString(file_config, "name");

            if (!file_path.string().starts_with("/"))
            {
                file_path = "" / file_path;
            }

            uint64_t file_size = getUInt64(file_config, "size");
            nodes[file_path] =
            {
                file_path.filename(),
                file_path.string(),
                file_size,
                true,
                false,
            };

            while (file_path.has_relative_path())
            {
                fs::path parent_path = file_path.parent_path();
                auto current_node = nodes.at(file_path);
                if (!nodes.contains(parent_path))
                {
                    nodes[parent_path] =
                    {parent_path.filename(),
                    parent_path.string(),
                    current_node.file_size,
                    false,
                    true,
                    {{file_path.string()}}
                    };
                }
                else
                {
                    nodes[parent_path].file_size += current_node.file_size;
                    nodes[parent_path].children.push_back(file_path.string());
                }

                file_path = parent_path;
            }
        }
    }
}

MetadataTransactionPtr MetadataStorageFromBackupFile::createTransaction()
{
    return std::make_shared<MetadataStorageFromBackupFileTransaction>(*this);
}

const String & MetadataStorageFromBackupFile::getPath() const
{
    static const String no_root;
    return no_root;
}

MetadataStorageType MetadataStorageFromBackupFile::getType() const
{
    return MetadataStorageType::Backup;
}

bool MetadataStorageFromBackupFile::exists(const String & path) const
{
    return nodes.contains(path);
}

bool MetadataStorageFromBackupFile::isFile(const String & path) const
{
    return exists(path) && nodes.at(path).is_file;
}

bool MetadataStorageFromBackupFile::isDirectory(const String & path) const
{
    return exists(path) && nodes.at(path).is_directory;
}

uint64_t MetadataStorageFromBackupFile::getFileSize(const String & path) const
{
    return nodes.at(path).file_size;
}

std::vector<String> MetadataStorageFromBackupFile::listDirectory(const String & path) const
{
    if (!isDirectory(path))
    {
        return {};
    }

    return nodes.at(path).children;
}

DirectoryIteratorPtr MetadataStorageFromBackupFile::iterateDirectory(const String & path) const
{
    std::vector<fs::path> dir_file_paths;

    if (!exists(path))
    {
        return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));
    }

    for (const auto& listed_path: nodes.at(path).children)
    {
        dir_file_paths.push_back(fs::path(listed_path));
    }

    return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));
}

StoredObjects MetadataStorageFromBackupFile::getStorageObjects(const String & path) const
{
    if (!exists(path))
    {
        return {};
    }

    auto node = nodes.at(path);
    // size_t object_size = getFileSize(path);
    // auto object_key = object_storage->generateObjectKeyForPath(path);
    return {StoredObject(path, path, node.file_size)};
}

const IMetadataStorage & MetadataStorageFromBackupFileTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

}
