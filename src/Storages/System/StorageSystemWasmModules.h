#pragma once
#include <Storages/IStorage.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class WasmModuleManager;

class StorageSystemWasmModules final : public IStorageSystemOneBlock
{
public:
    StorageSystemWasmModules(const StorageID & table_id_, ColumnsDescription columns_description_, WasmModuleManager & wasm_module_manager_);

    String getName() const override { return "StorageSystemWebAssemblyModules"; }

    static ColumnsDescription getColumnsDescription();

    bool supportsColumnsMask() const override { return true; }
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const override;

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/) override;

    bool isSystemStorage() const override { return true; }

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;
    bool supportsDelete() const override { return true; }

private:
    WasmModuleManager & wasm_module_manager;
};


}
