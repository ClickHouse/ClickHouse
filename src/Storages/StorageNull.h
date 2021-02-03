#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Pipe.h>


namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull final : public ext::shared_ptr_helper<StorageNull>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageNull>;
public:
    std::string getName() const override { return "Null"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo &,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t,
        unsigned) override
    {
        return Pipe(
            std::make_shared<NullSource>(metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID())));
    }

    bool supportsParallelInsert() const override { return true; }

    BlockOutputStreamPtr write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context &) override
    {
        return std::make_shared<NullBlockOutputStream>(metadata_snapshot->getSampleBlock());
    }

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) const override;

    void alter(const AlterCommands & params, const Context & context, TableLockHolder & table_lock_holder) override;

    std::optional<UInt64> totalRows(const Settings &) const override
    {
        return {0};
    }
    std::optional<UInt64> totalBytes(const Settings &) const override
    {
        return {0};
    }

private:

protected:
    StorageNull(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
        : IStorage(table_id_)
    {
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(columns_description_);
        metadata_.setConstraints(constraints_);
        setInMemoryMetadata(metadata_);
    }
};

}
