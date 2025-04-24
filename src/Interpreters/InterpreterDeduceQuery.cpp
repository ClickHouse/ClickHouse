#include <Access/Common/AccessRightsElement.h>
#include <Columns/IColumnUnique.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDeduceQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTDeduceQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include "Common/Logger.h"
#include <Common/typeid_cast.h>
#include "Columns/IColumn.h"

#include <Interpreters/processColumnTransformers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
extern const int THERE_IS_NO_COLUMN;
}


BlockIO InterpreterDeduceQuery::execute()
{
    const auto & ast = query_ptr->as<ASTDeduceQuery &>();
    auto context = getContext();
    context->checkAccess(getRequiredAccess());

    auto table_id = context->resolveStorageID(ast);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, context);
    checkStorageSupportsTransactionsIfNeeded(table, context);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();
    auto storage_snapshot = table->getStorageSnapshot(metadata_snapshot, context);

    if (auto * snapshot_data = dynamic_cast<MergeTreeData::SnapshotData *>(storage_snapshot->data.get()))
        snapshot_data->parts = {};

    if (!ast.col_to_deduce)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "no columns to deduce specified");
    }
    auto result = table->deduce(query_ptr, (ast.col_to_deduce)->as<ASTIdentifier>()->name(), metadata_snapshot, context);

    Block block(ColumnsWithTypeAndName{
        ColumnWithTypeAndName(DataTypePtr(new DataTypeString()), "partition"),
        ColumnWithTypeAndName(DataTypePtr(new DataTypeString()), "hypothesis")});

    MutableColumns columns = block.cloneEmptyColumns();
    for (const auto & [part_name, hypothesis_str] : result)
    {
        columns[0]->insert(part_name);
        columns[1]->insert(hypothesis_str);
    }
    BlockIO res;
    size_t num_rows = result.size();
    auto source = std::make_shared<SourceFromSingleChunk>(block, Chunk(std::move(columns), num_rows));
    res.pipeline = QueryPipeline(std::move(source));
    return res;
}


AccessRightsElements InterpreterDeduceQuery::getRequiredAccess() const
{
    const auto & deduce = query_ptr->as<const ASTDeduceQuery &>();
    AccessRightsElements required_access;
    required_access.emplace_back(AccessType::DEDUCE, deduce.getDatabase(), deduce.getTable());
    return required_access;
}

void registerInterpreterDeduceQuery(InterpreterFactory & factory)
{
    auto create_fn
        = [](const InterpreterFactory::Arguments & args) { return std::make_unique<InterpreterDeduceQuery>(args.query, args.context); };
    factory.registerInterpreter("InterpreterDeduceQuery", create_fn);
}
}
