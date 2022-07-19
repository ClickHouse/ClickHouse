#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCheckQuery.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCheckQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <algorithm>


namespace DB
{

namespace
{

NamesAndTypes getBlockStructure()
{
    return {
        {"part_path", std::make_shared<DataTypeString>()},
        {"is_passed", std::make_shared<DataTypeUInt8>()},
        {"message", std::make_shared<DataTypeString>()},
    };
}

}


InterpreterCheckQuery::InterpreterCheckQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterCheckQuery::execute()
{
    const auto & check = query_ptr->as<ASTCheckQuery &>();
    auto table_id = getContext()->resolveStorageID(check, Context::ResolveOrdinary);

    getContext()->checkAccess(AccessType::SHOW_TABLES, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto check_results = table->checkData(query_ptr, getContext());

    Block block;
    if (getContext()->getSettingsRef().check_query_single_value_result)
    {
        bool result = std::all_of(check_results.begin(), check_results.end(), [] (const CheckResult & res) { return res.success; });
        auto column = ColumnUInt8::create();
        column->insertValue(static_cast<UInt64>(result));
        block = Block{{std::move(column), std::make_shared<DataTypeUInt8>(), "result"}};
    }
    else
    {
        auto block_structure = getBlockStructure();
        auto path_column = block_structure[0].type->createColumn();
        auto is_passed_column = block_structure[1].type->createColumn();
        auto message_column = block_structure[2].type->createColumn();

        for (const auto & check_result : check_results)
        {
            path_column->insert(check_result.fs_path);
            is_passed_column->insert(static_cast<UInt8>(check_result.success));
            message_column->insert(check_result.failure_message);
        }

        block = Block({
            {std::move(path_column), block_structure[0].type, block_structure[0].name},
            {std::move(is_passed_column), block_structure[1].type, block_structure[1].name},
            {std::move(message_column), block_structure[2].type, block_structure[2].name}});
    }

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(block)));

    return res;
}

}
