#include <Storages/System/StorageSystemTokenizers.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/TokenizerFactory.h>


namespace DB
{

ColumnsDescription StorageSystemTokenizers::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the tokenizer"}
    };
}

StorageSystemTokenizers::StorageSystemTokenizers(const StorageID & table_id)
    : IStorageSystemOneBlock(table_id, getColumnsDescription())
{
}

void StorageSystemTokenizers::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto & tokenizer_factory = TokenizerFactory::instance();
    const auto & tokenizers = tokenizer_factory.getAllTokenizers();

    for (const auto & tokenizer : tokenizers)
        res_columns[0]->insert(tokenizer.first);
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemTokenizers) }
