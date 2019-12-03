#pragma once

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>


namespace DB
{

/** When writing, does nothing.
  * When reading, returns nothing.
  */
class StorageNull : public ext::shared_ptr_helper<StorageNull>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageNull>;
public:
    std::string getName() const override { return "Null"; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo &,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processing_stage*/,
        size_t,
        unsigned) override
    {
        return { std::make_shared<NullBlockInputStream>(getSampleBlockForColumns(column_names)) };
    }

    BlockOutputStreamPtr write(const ASTPtr &, const Context &) override
    {
        return std::make_shared<NullBlockOutputStream>(getSampleBlock());
    }

    void alter(
        const AlterCommands & params, const Context & context, TableStructureWriteLockHolder & table_lock_holder) override;

private:

protected:
    StorageNull(String database_name_, String table_name_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
        : IStorage({database_name_, table_name_})
    {
        setColumns(std::move(columns_description_));
        setConstraints(std::move(constraints_));
    }
};

}
