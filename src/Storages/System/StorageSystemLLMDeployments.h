#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemLLMDeployments final : public IStorageSystemOneBlock
{
public:
    StorageSystemLLMDeployments(const StorageID & table_id, ColumnsDescription columns_description);

    String getName() const override
    {
        return "SystemLLMDeployments";
    }
    static ColumnsDescription getColumnsDescription();
    void fillData(MutableColumns & columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr context, bool) override;
    bool supportsDelete() const override
    {
        return true;
    }
    void checkMutationIsPossible(const MutationCommands & commands, const Settings &) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;
};

}
