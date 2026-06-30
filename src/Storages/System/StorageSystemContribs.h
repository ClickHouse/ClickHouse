#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** System table "contribs" with a list of third-party libraries from the `contrib/` directory:
  * directory name, path, pinned commit, submodule URL, upstream URL, fork flag, and version.
  */
class StorageSystemContribs final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemContribs";
    }

    static ColumnsDescription getColumnsDescription();
};
}
