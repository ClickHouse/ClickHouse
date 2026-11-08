#include <Storages/System/StorageSystemWasmModules.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>


#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>

#include <IO/copyData.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/WasmModuleManager.h>
#include <Storages/MutationCommands.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int BAD_ARGUMENTS;
}

ColumnsDescription StorageSystemWasmModules::getColumnsDescription()
{
    return ColumnsDescription{
        {"name", std::make_shared<DataTypeString>(),
            "Name of the WebAssembly module"},
        {"code", std::make_shared<DataTypeString>(),
            "Binary WebAssembly module code in raw format. "
            "This column is write-only and contains empty values when read."},
        {"hash", std::make_shared<DataTypeUInt256>(),
            "SHA256 hash of the module's binary code, used for integrity verification. "
            "Contains zero if the module exists on disk but is not currently loaded into memory"},
    };
}

class WasmModulesSink final : public SinkToStorage
{
public:
    WasmModulesSink(SharedHeader header_, WasmModuleManager & wasm_module_manager_)
        : SinkToStorage(std::move(header_))
        , wasm_module_manager(wasm_module_manager_)
    {}

    String getName() const override { return "WasmModulesSink"; }

    void consume(Chunk & chunk) override
    {
        const auto & columns = chunk.getColumns();
        if (columns.size() != 3)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Expected 3 columns, got {}", columns.size());

        const auto * name_column = assert_cast<const ColumnString *>(columns[0].get());
        const auto * code_column = assert_cast<const ColumnString *>(columns[1].get());
        const auto * hash_column = assert_cast<const ColumnVector<UInt256> *>(columns[2].get());
        size_t num_rows = chunk.getNumRows();
        for (size_t i = 0; i < num_rows; ++i)
        {
            std::string_view name = name_column->getDataAt(i);
            std::string_view code = code_column->getDataAt(i);
            UInt256 expected_hash = hash_column->getElement(i);
            wasm_module_manager.saveModule(name, code, expected_hash);
        }
    }

private:
    WasmModuleManager & wasm_module_manager;
};


StorageSystemWasmModules::StorageSystemWasmModules(const StorageID & table_id_, ColumnsDescription columns_description_, WasmModuleManager & wasm_module_manager_)
    : IStorageSystemOneBlock(table_id_, std::move(columns_description_))
    , wasm_module_manager(wasm_module_manager_)
{}

SinkToStoragePtr StorageSystemWasmModules::write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr /* context */, bool /*async_insert*/)
{
    auto write_header = std::make_shared<Block>();
    for (const auto & [name, type] : StorageSystemWasmModules::getColumnsDescription().getInsertable())
    {
        write_header->insert(ColumnWithTypeAndName(type, name));
    }
    return std::make_shared<WasmModulesSink>(std::move(write_header), wasm_module_manager);
}

void StorageSystemWasmModules::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const
{
    if (columns_mask.size() != getColumnsDescription().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} columns, got {}", getColumnsDescription().size(), columns_mask.size());
    if (res_columns.size() != std::accumulate(columns_mask.begin(), columns_mask.end(), 0u))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} columns, got {}", std::accumulate(columns_mask.begin(), columns_mask.end(), 0u), res_columns.size());

    for (const auto & [module_name, module_hash] : wasm_module_manager.getModulesList())
    {
        size_t column_idx = 0;

        if (columns_mask[0])
            res_columns[column_idx++]->insert(module_name);

        /// 'code' column is write-only, insert default value
        if (columns_mask[1])
            res_columns[column_idx++]->insertDefault();

        if (columns_mask[2])
            res_columns[column_idx++]->insert(module_hash);
    }
}

void StorageSystemWasmModules::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER is not supported by storage {}", getName());
}

void StorageSystemWasmModules::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER is not supported by storage {}", getName());
}

static std::optional<String> getModuleNameToDeleteFromAst(const MutationCommands & commands)
{
    if (commands.size() != 1)
        return {};

    const auto & command = commands.front();
    if (command.type != MutationCommand::DELETE || command.partition || !command.predicate)
        return {};

    const auto * func = command.predicate->as<ASTFunction>();
    if (!func || func->name != "equals" || func->arguments->children.size() != 2)
        return {};

    const auto & args = func->arguments->children;
    if (args[0]->as<ASTIdentifier>()->full_name != "name")
        return {};

    const auto * literal = args[1]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        return {};

    return literal->value.safeGet<String>();
}

void StorageSystemWasmModules::checkMutationIsPossible(const MutationCommands & commands, const Settings &) const
{
    if (getModuleNameToDeleteFromAst(commands).has_value())
        return;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Only deletion of a module by name is supported. "
        "Use query `DELETE FROM {} WHERE name == 'module_name'`", getStorageID().getFullTableName());
}

void StorageSystemWasmModules::mutate(const MutationCommands & commands, ContextPtr)
{
    auto module_name = getModuleNameToDeleteFromAst(commands);
    if (!module_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected single DELETE command with WHERE clause, got {}", commands.toString(/* with_pure_metadata_commands */ true));

    wasm_module_manager.deleteModuleIfExists(*module_name);
}

}
