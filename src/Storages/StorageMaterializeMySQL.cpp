#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Storages/StorageMaterializeMySQL.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <Processors/Pipe.h>
#include <Processors/Transforms/FilterTransform.h>

namespace DB
{

StorageMaterializeMySQL::StorageMaterializeMySQL(const StoragePtr & nested_storage_, const DatabaseMaterializeMySQL * database_)
    : StorageProxy(nested_storage_->getStorageID()), nested_storage(nested_storage_), database(database_)
{
    auto nested_memory_metadata = nested_storage->getInMemoryMetadata();
    StorageInMemoryMetadata in_memory_metadata;
    in_memory_metadata.setColumns(nested_memory_metadata.getColumns());
    setInMemoryMetadata(in_memory_metadata);
}

Pipe StorageMaterializeMySQL::read(
    const Names & column_names,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
{
    /// If the background synchronization thread has exception.
    database->rethrowExceptionIfNeed();

    NameSet column_names_set = NameSet(column_names.begin(), column_names.end());
    auto lock = nested_storage->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    const StorageMetadataPtr & nested_metadata = nested_storage->getInMemoryMetadataPtr();

    Block nested_header = nested_metadata->getSampleBlock();
    ColumnWithTypeAndName & sign_column = nested_header.getByPosition(nested_header.columns() - 2);
    ColumnWithTypeAndName & version_column = nested_header.getByPosition(nested_header.columns() - 1);

    if (ASTSelectQuery * select_query = query_info.query->as<ASTSelectQuery>(); select_query && !column_names_set.count(version_column.name))
    {
        auto & tables_in_select_query = select_query->tables()->as<ASTTablesInSelectQuery &>();

        if (!tables_in_select_query.children.empty())
        {
            auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();

            if (tables_element.table_expression)
                tables_element.table_expression->as<ASTTableExpression &>().final = true;
        }
    }

    String filter_column_name;
    Names require_columns_name = column_names;
    ASTPtr expressions = std::make_shared<ASTExpressionList>();
    if (column_names_set.empty() || !column_names_set.count(sign_column.name))
    {
        require_columns_name.emplace_back(sign_column.name);

        const auto & sign_column_name = std::make_shared<ASTIdentifier>(sign_column.name);
        const auto & fetch_sign_value = std::make_shared<ASTLiteral>(Field(Int8(1)));

        expressions->children.emplace_back(makeASTFunction("equals", sign_column_name, fetch_sign_value));
        filter_column_name = expressions->children.back()->getColumnName();

        for (const auto & column_name : column_names)
            expressions->children.emplace_back(std::make_shared<ASTIdentifier>(column_name));
    }

    Pipe pipe = nested_storage->read(require_columns_name, nested_metadata, query_info, context, processed_stage, max_block_size, num_streams);
    pipe.addTableLock(lock);

    if (!expressions->children.empty() && !pipe.empty())
    {
        Block pipe_header = pipe.getHeader();
        auto syntax = TreeRewriter(context).analyze(expressions, pipe_header.getNamesAndTypesList());
        ExpressionActionsPtr expression_actions = ExpressionAnalyzer(expressions, syntax, context).getActions(true);

        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<FilterTransform>(header, expression_actions, filter_column_name, false);
        });
    }

    return pipe;
}

NamesAndTypesList StorageMaterializeMySQL::getVirtuals() const
{
    /// If the background synchronization thread has exception.
    database->rethrowExceptionIfNeed();
    return nested_storage->getVirtuals();
}

IStorage::ColumnSizeByName StorageMaterializeMySQL::getColumnSizes() const
{
    auto sizes = nested_storage->getColumnSizes();
    auto nested_header = nested_storage->getInMemoryMetadataPtr()->getSampleBlock();
    String sign_column_name = nested_header.getByPosition(nested_header.columns() - 2).name;
    String version_column_name = nested_header.getByPosition(nested_header.columns() - 1).name;
    sizes.erase(sign_column_name);
    sizes.erase(version_column_name);
    return sizes;
}

}

#endif
