#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper_info` system table, which allows you to get information about all zookeepers in the config.
*/
class StorageSystemZooKeeperInfo final : public IStorageSystemOneBlock
{
public:
std::string getName() const override { return "StorageSystemZooKeeperInfo"; }

static ColumnsDescription getColumnsDescription();

protected:
using IStorageSystemOneBlock::IStorageSystemOneBlock;

void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;

std::expected<String,String> sendFourLetterCommand(String host, String port, String command) const;

};

}
