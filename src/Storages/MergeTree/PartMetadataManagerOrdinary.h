#pragma once

#include <Storages/MergeTree/IPartMetadataManager.h>

namespace DB
{

class PartMetadataManagerOrdinary : public IPartMetadataManager
{
public:
    explicit PartMetadataManagerOrdinary(const IMergeTreeDataPart * part_) : IPartMetadataManager(part_) {}

    ~PartMetadataManagerOrdinary() override = default;

    std::unique_ptr<ReadBuffer> read(const String & file_name) const override;
    std::unique_ptr<ReadBuffer> readIfExists(const String & file_name) const override;

    bool exists(const String & file_name) const override;

    void deleteAll(bool /*include_projection*/) override {}

    void assertAllDeleted(bool /*include_projection*/) const override {}

    void updateAll(bool /*include_projection*/) override {}

    std::unordered_map<String, uint128> check() const override { return {}; }
};


}
