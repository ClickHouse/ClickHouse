#pragma once

#include <optional>
#include <string>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include "CommonPathPrefixKeyGenerator.h"

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
template <typename BaseObjectStorage>
class PlainRewritableObjectStorage : public BaseObjectStorage
{
public:
    template <typename... Args>
    explicit PlainRewritableObjectStorage(MetadataStorageMetrics && metadata_storage_metrics_, Args &&... args)
        : BaseObjectStorage(std::forward<Args>(args)...)
        , metadata_storage_metrics(std::move(metadata_storage_metrics_))
        /// A basic key generator is required for checking S3 capabilities,
        /// it will be reset later by metadata storage.
        , key_generator(createObjectStorageKeysGeneratorAsIsWithPrefix(BaseObjectStorage::getCommonKeyPrefix()))
    {
    }

    std::string getName() const override { return "PlainRewritable" + BaseObjectStorage::getName(); }

    const MetadataStorageMetrics & getMetadataStorageMetrics() const override { return metadata_storage_metrics; }

    bool isWriteOnce() const override { return false; }

    bool isPlain() const override { return true; }

    ObjectStorageKey generateObjectKeyForPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

    ObjectStorageKey
    generateObjectKeyPrefixForDirectoryPath(const std::string & path, const std::optional<std::string> & key_prefix) const override;

    void setKeysGenerator(ObjectStorageKeysGeneratorPtr gen) override { key_generator = gen; }

private:
    MetadataStorageMetrics metadata_storage_metrics;
    ObjectStorageKeysGeneratorPtr key_generator;
};


template <typename BaseObjectStorage>
ObjectStorageKey PlainRewritableObjectStorage<BaseObjectStorage>::generateObjectKeyForPath(
    const std::string & path, const std::optional<std::string> & key_prefix) const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");

    return key_generator->generate(path, /* is_directory */ false, key_prefix);
}

template <typename BaseObjectStorage>
ObjectStorageKey PlainRewritableObjectStorage<BaseObjectStorage>::generateObjectKeyPrefixForDirectoryPath(
    const std::string & path, const std::optional<std::string> & key_prefix) const
{
    if (!key_generator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Key generator is not set");

    return key_generator->generate(path, /* is_directory */ true, key_prefix);
}
}
