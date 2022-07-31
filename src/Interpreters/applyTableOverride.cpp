#include <Interpreters/applyTableOverride.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTTableOverrides.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>

namespace DB
{

void applyTableOverrideToCreateQuery(const ASTTableOverride & override, ASTCreateQuery * create_query)
{
    if (auto * columns = override.columns)
    {
        if (!create_query->columns_list)
            create_query->set(create_query->columns_list, std::make_shared<ASTColumns>());
        if (columns->columns)
        {
            for (const auto & override_column_ast : columns->columns->children)
            {
                auto * override_column = override_column_ast->as<ASTColumnDeclaration>();
                if (!override_column)
                    continue;
                if (!create_query->columns_list->columns)
                    create_query->columns_list->set(create_query->columns_list->columns, std::make_shared<ASTExpressionList>());
                auto & dest_children = create_query->columns_list->columns->children;
                auto exists = std::find_if(dest_children.begin(), dest_children.end(), [&](ASTPtr node) -> bool
                {
                    return node->as<ASTColumnDeclaration>()->name == override_column->name;
                });
                /// For columns, only allow adding ALIAS (non-physical) for now.
                /// TODO: This logic should instead be handled by validation that is
                ///       executed from InterpreterCreateQuery / InterpreterAlterQuery.
                if (exists == dest_children.end())
                {
                    if (override_column->default_specifier == "ALIAS")
                        dest_children.emplace_back(override_column_ast);
                }
                else
                    *exists = override_column_ast;
            }
        }
        if (columns->indices)
        {
            for (const auto & override_index_ast : columns->indices->children)
            {
                auto * override_index = override_index_ast->as<ASTIndexDeclaration>();
                if (!override_index)
                    continue;
                if (!create_query->columns_list->indices)
                    create_query->columns_list->set(create_query->columns_list->indices, std::make_shared<ASTExpressionList>());
                auto & dest_children = create_query->columns_list->indices->children;
                auto exists = std::find_if(dest_children.begin(), dest_children.end(), [&](ASTPtr node) -> bool
                {
                    return node->as<ASTIndexDeclaration>()->name == override_index->name;
                });
                if (exists == dest_children.end())
                    dest_children.emplace_back(override_index_ast);
                else
                    *exists = override_index_ast;
            }
        }
        if (columns->constraints)
        {
            for (const auto & override_constraint_ast : columns->constraints->children)
            {
                auto * override_constraint = override_constraint_ast->as<ASTConstraintDeclaration>();
                if (!override_constraint)
                    continue;
                if (!create_query->columns_list->constraints)
                    create_query->columns_list->set(create_query->columns_list->constraints, std::make_shared<ASTExpressionList>());
                auto & dest_children = create_query->columns_list->constraints->children;
                auto exists = std::find_if(dest_children.begin(), dest_children.end(), [&](ASTPtr node) -> bool
                {
                    return node->as<ASTConstraintDeclaration>()->name == override_constraint->name;
                });
                if (exists == dest_children.end())
                    dest_children.emplace_back(override_constraint_ast);
                else
                    * exists = override_constraint_ast;
            }
        }
        if (columns->projections)
        {
            for (const auto & override_projection_ast : columns->projections->children)
            {
                auto * override_projection = override_projection_ast->as<ASTProjectionDeclaration>();
                if (!override_projection)
                    continue;
                if (!create_query->columns_list->projections)
                    create_query->columns_list->set(create_query->columns_list->projections, std::make_shared<ASTExpressionList>());
                auto & dest_children = create_query->columns_list->projections->children;
                auto exists = std::find_if(dest_children.begin(), dest_children.end(), [&](ASTPtr node) -> bool
                {
                    return node->as<ASTProjectionDeclaration>()->name == override_projection->name;
                });
                if (exists == dest_children.end())
                    dest_children.emplace_back(override_projection_ast);
                else
                    *exists = override_projection_ast;
            }
        }
    }
    if (auto * storage = override.storage)
    {
        if (!create_query->storage)
            create_query->set(create_query->storage, std::make_shared<ASTStorage>());
        if (storage->partition_by)
            create_query->storage->set(create_query->storage->partition_by, storage->partition_by->clone());
        if (storage->primary_key)
            create_query->storage->set(create_query->storage->primary_key, storage->primary_key->clone());
        if (storage->order_by)
            create_query->storage->set(create_query->storage->order_by, storage->order_by->clone());
        if (storage->sample_by)
            create_query->storage->set(create_query->storage->sample_by, storage->sample_by->clone());
        if (storage->ttl_table)
            create_query->storage->set(create_query->storage->ttl_table, storage->ttl_table->clone());
        // No support for overriding ENGINE and SETTINGS
    }

}

}
