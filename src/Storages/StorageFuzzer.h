#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>

#include <DataTypes/DataTypeString.h>

namespace DB
{


class StorageFuzzer final : public shared_ptr_helper<StorageFuzzer>, public IStorage
{
    friend struct shared_ptr_helper<StorageFuzzer>;
public:
    std::string getName() const override { return "Fuzzer"; }

    Pipe read(const Names &, const StorageMetadataPtr &, SelectQueryInfo &,
        ContextPtr, QueryProcessingStage::Enum, size_t, unsigned) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr) override;

    void checkAlterIsPossible(const AlterCommands &, ContextPtr) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

    void alter(const AlterCommands &, ContextPtr, TableLockHolder &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

    std::optional<UInt64> totalRows(const Settings &) const override { return {0}; }
    std::optional<UInt64> totalBytes(const Settings &) const override { return {0}; }

private:

protected:
    StorageFuzzer(
        const StorageID & table_id_, ColumnsDescription, ConstraintsDescription, const String &)
        : IStorage(table_id_)
    {
        ColumnDescription column{"query", std::make_shared<DataTypeString>()};
        ColumnsDescription description;
        description.add(std::move(column));

        StorageInMemoryMetadata metadata;
        metadata.setColumns(std::move(description));
        setInMemoryMetadata(metadata);
    }
};

}
