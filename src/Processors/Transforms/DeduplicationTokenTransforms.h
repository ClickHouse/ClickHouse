#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Core/SettingsEnums.h>


namespace DB
{

class RestoreChunkInfosTransform : public ISimpleTransform
{
public:
    RestoreChunkInfosTransform(Chunk::ChunkInfoCollection chunk_infos_, SharedHeader header_);

    String getName() const override { return "RestoreChunkInfosTransform"; }

    void transform(Chunk & chunk) override;

private:
    Chunk::ChunkInfoCollection chunk_infos;
};


class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

class AddDeduplicationInfoTransform : public ISimpleTransform
{
    InsertDependenciesBuilderConstPtr insert_dependencies;
    StorageIDMaybeEmpty root_view_id;
    std::string user_token;
    size_t block_number = 0;
    InsertDeduplicationVersions unification_stage = InsertDeduplicationVersions::NEW_UNIFIED_HASHES;
public:
    explicit AddDeduplicationInfoTransform(SharedHeader header_);

    AddDeduplicationInfoTransform(
        InsertDependenciesBuilderConstPtr insert_dependencies_,
        StorageIDMaybeEmpty root_view_id_,
        std::string user_token_,
        InsertDeduplicationVersions unification_stage_,
        SharedHeader header_);

    String getName() const override { return "AddDeduplicationInfoTransform"; }

    void transform(Chunk & chunk) override;
};


class RedefineDeduplicationInfoWithDataHashTransform : public ISimpleTransform
{
public:
    explicit RedefineDeduplicationInfoWithDataHashTransform(SharedHeader header_);

    String getName() const override { return "RedefineDeduplicationInfoWithDataHashTransform"; }

    void transform(Chunk & chunk) override;
};


struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class SelectPartitionTransform : public ISimpleTransform
{
    std::string partition_id;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;

public:
    SelectPartitionTransform(std::string partition_id_, StorageMetadataPtr metadata_snapshot_, ContextPtr contex_, SharedHeader header_);

    String getName() const override { return "SelectPartitionTransform"; }

    void transform(Chunk &) override;
};


class UpdateDeduplicationInfoWithViewIDTransform : public ISimpleTransform
{
public:
    UpdateDeduplicationInfoWithViewIDTransform(StorageIDMaybeEmpty view_id_, SharedHeader header_);

    String getName() const override { return "UpdateDeduplicationInfoWithViewIDTransform"; }

    void transform(Chunk & chunk) override;

private:
    StorageIDMaybeEmpty view_id;
    size_t block_number = 0;
};


}
