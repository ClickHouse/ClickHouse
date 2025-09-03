#pragma once

#include <Storages/IStorage.h>

namespace DB
{

enum class StorageAliasBehaviourKind : uint8_t
{
    USE_ORIGINAL_TABLE = 0,
    USE_ALIAS_TABLE = 1,
    EXCEPTION = 2,
};

/* An alias for another table. */
class StorageAlias final : public IStorage
{
public:
    StorageAlias(const StorageID & table_id_, const StorageID & ref_table_id_);
    std::string getName() const override { return "Alias"; }

    std::shared_ptr<IStorage> getReferenceTable(ContextPtr context) const;

    void alter(const AlterCommands &, ContextPtr, AlterLockHolder &) override;

    static void modifyContextByQueryAST(ASTPtr query, ContextMutablePtr context);
private:
    /// Store ddatabase.table or UUID of the reference table.
    /// ref_table_id.uuid is Nil, find the table by ref_table_id.database_name, ref_table_id.table_name;
    /// otherwise find the table by ref_table_id.uuid.
    StorageID ref_table_id;
};

}
