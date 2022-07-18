#include <Storages/ReadFinalForExternalReplicaStorage.h>

#if USE_MYSQL || USE_LIBPQXX

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Interpreters/Context.h>


namespace DB
{

bool needRewriteQueryWithFinalForStorage(const Names & column_names, const StoragePtr & storage)
{
    const StorageMetadataPtr & metadata = storage->getInMemoryMetadataPtr();
    Block header = metadata->getSampleBlock();
    ColumnWithTypeAndName & version_column = header.getByPosition(header.columns() - 1);
    return std::find(column_names.begin(), column_names.end(), version_column.name) == column_names.end();
}

Pipe readFinalFromNestedStorage(
    StoragePtr nested_storage,
    const Names & column_names,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
{
    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
    auto lock = nested_storage->lockForShare(context->getCurrentQueryId(), context->getSettingsRef().lock_acquire_timeout);
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
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));

        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
        filter_column_name = expressions->children.back()->getColumnName();
    }

    auto nested_snapshot = nested_storage->getStorageSnapshot(nested_metadata, context);
    Pipe pipe = nested_storage->read(require_columns_name, nested_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    pipe.addTableLock(lock);
    pipe.addStorageHolder(nested_storage);

    if (!expressions->children.empty() && !pipe.empty())
    {
        Block pipe_header = pipe.getHeader();
        auto syntax = TreeRewriter(context).analyze(expressions, pipe_header.getNamesAndTypesList());
        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true /* add_aliases */, false /* project_result */);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(header, expression_actions, filter_column_name, false);
        });
    }

    return pipe;
}
}

#endif
