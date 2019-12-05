#include <Access/AccessRightsElement.h>
#include <Common/typeid_cast.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterFreezeQuery.h>
#include <Parsers/ASTFreezeQuery.h>
#include <Storages/IStorage.h>


namespace DB
{

BlockIO InterpreterFreezeQuery::execute()
{
    BlockIO res;
    res.in = executeImpl();
    return res;
}


Block InterpreterFreezeQuery::getSampleBlock()
{
    Block block{
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "backup_name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "partition"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "partition_id"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "part_name"),
        ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "backup_path"),
    };

    return block;
}


BlockInputStreamPtr InterpreterFreezeQuery::executeImpl()
{
    const auto & ast = query_ptr->as<ASTFreezeQuery &>();

    context.checkAccess(AccessType::FREEZE_PARTITION, ast.database, ast.table);

    auto table_id = context.resolveStorageID(ast, Context::ResolveOrdinary);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id);

    FreezeResult freeze_result = table->freeze(ast.partition, ast.with_name, context);

    Block sample_block = getSampleBlock();
    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    for (const FreezeResult::PartInfo & frozen_part : freeze_result.frozen_parts)
    {
        res_columns[0]->insert(freeze_result.backup_name);
        res_columns[1]->insert(frozen_part.partition);
        res_columns[2]->insert(frozen_part.partition_id);
        res_columns[3]->insert(frozen_part.part_name);
        res_columns[4]->insert(frozen_part.backup_path);
    }

    return std::make_shared<OneBlockInputStream>(sample_block.cloneWithColumns(std::move(res_columns)));
}

}
