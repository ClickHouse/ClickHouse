#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/NullBlockInputStream.h>
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

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t,
        unsigned) override
    {
        Pipes pipes;
        pipes.emplace_back(std::make_shared<NullSource>(getSampleBlockForColumns(column_names)));
        return pipes;
    }

    BlockOutputStreamPtr write(const ASTPtr &, const Context &) override
    {
        return std::make_shared<NullBlockOutputStream>(getSampleBlock());
    }

    void checkAlterIsPossible(const AlterCommands & commands, const Settings & /* settings */) override;

    void alter(const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

    std::optional<UInt64> totalRows() const override
    {
        return {0};
    }
    std::optional<UInt64> totalBytes() const override
    {
        return {0};
    }

private:

protected:
    StorageNull(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
        : IStorage(table_id_)
    {
        setColumns(std::move(columns_description_));
        setConstraints(std::move(constraints_));
    }
};

}
