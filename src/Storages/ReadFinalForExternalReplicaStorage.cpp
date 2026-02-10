#include <Storages/ReadFinalForExternalReplicaStorage.h>

#if USE_MYSQL || USE_LIBPQXX

#include <Core/Settings.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

bool needRewriteQueryWithFinalForStorage(const Names & column_names, const StoragePtr & storage)
{
    const StorageMetadataPtr & metadata = storage->getInMemoryMetadataPtr();
    Block header = metadata->getSampleBlock();
    ColumnWithTypeAndName & version_column = header.getByPosition(header.columns() - 1);
    return std::find(column_names.begin(), column_names.end(), version_column.name) == column_names.end();
}

void readFinalFromNestedStorage(
    QueryPlan & query_plan,
    StoragePtr nested_storage,
    const Names & column_names,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
    auto lock = nested_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    const auto & nested_metadata = nested_storage->getInMemoryMetadataPtr();

    Block nested_header = nested_metadata->getSampleBlock();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);

    String filter_column_name;
    Names require_columns_name = column_names;
    ASTPtr expressions = std::make_shared<ASTExpressionList>();
    if (column_names_set.empty() || !column_names_set.contains(sign_column.name))
    {
        require_columns_name.emplace_back(sign_column.name);

        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(static_cast<Int8>(1)));

        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
        filter_column_name = expressions->children.back()->getColumnName();
    }

    auto nested_snapshot = nested_storage->getStorageSnapshot(nested_metadata, context);
    nested_storage->read(
        query_plan, require_columns_name, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    if (!query_plan.isInitialized())
    {
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, nested_header, query_info);
        return;
    }

    query_plan.addTableLock(lock);
    query_plan.addStorageHolder(nested_storage);

    if (!expressions->children.empty())
    {
        const auto & header = query_plan.getCurrentHeader();
        auto syntax = TreeRewriter(context).analyze(expressions, header.getNamesAndTypesList());
        auto actions = ExpressionAnalyzer(expressions, syntax, context).getActionsDAG(true /* add_aliases */, false /* project_result */);

        auto step = std::make_unique<FilterStep>(
            query_plan.getCurrentHeader(),
            std::move(actions),
            filter_column_name,
            false);

        step->setStepDescription("Filter columns");
        query_plan.addStep(std::move(step));
    }
}
}

#endif
