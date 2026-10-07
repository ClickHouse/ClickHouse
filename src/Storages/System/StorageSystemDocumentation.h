#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/** Collects the embedded documentation of the uniform components of the system
  * (functions, table engines, data types, etc.) into a single table.
  *
  * Every row corresponds to one entity and contains its name, the kind of the entity,
  * and its embedded reference documentation rendered as Markdown. Website pages may
  * add MDX-only content outside their generated bodies.
  *
  * It is meant to back the interactive `help` command of the client, but is useful on its own.
  */
class StorageSystemDocumentation final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDocumentation"; }

    static ColumnsDescription getColumnsDescription();
};

}
