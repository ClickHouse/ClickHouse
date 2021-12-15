#include <IO/Operators.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTTableOverrides.h>

namespace DB
{

ASTPtr ASTTableOverride::clone() const
{
    auto res = std::make_shared<ASTTableOverride>(*this);
    res->children.clear();
    res->table_name = table_name;
    if (columns)
        res->set(res->columns, columns->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    return res;
}

void ASTTableOverride::formatImpl(const FormatSettings & settings_, FormatState & state, FormatStateStacked frame) const
{
    FormatSettings settings = settings_;
    settings.always_quote_identifiers = true;
    String nl_or_nothing = settings.one_line ? "" : "\n";
    String nl_or_ws = settings.one_line ? " " : "\n";
    String hl_keyword = settings.hilite ? hilite_keyword : "";
    String hl_none = settings.hilite ? hilite_none : "";

    settings.ostr << hl_keyword << "TABLE OVERRIDE " << hl_none;
    ASTIdentifier(table_name).formatImpl(settings, state, frame);
    if (!columns && (!storage || storage->children.empty()))
        return;
    auto override_frame = frame;
    ++override_frame.indent;
    settings.ostr << nl_or_ws << '(' << nl_or_nothing;
    String indent_str = settings.one_line ? "" : String(4 * override_frame.indent, ' ');
    size_t override_elems = 0;
    if (columns)
    {
        FormatStateStacked columns_frame = override_frame;
        columns_frame.expression_list_always_start_on_new_line = true;
        settings.ostr << indent_str << hl_keyword << "COLUMNS" << hl_none << nl_or_ws << indent_str << "(";
        columns->formatImpl(settings, state, columns_frame);
        settings.ostr << nl_or_nothing << indent_str << ")";
        ++override_elems;
    }
    if (storage)
    {
        const auto & format_storage_elem = [&](IAST * elem, const String & elem_name)
        {
            if (elem)
            {
                settings.ostr << (override_elems++ ? nl_or_ws : "")
                              << indent_str
                              << hl_keyword << elem_name << hl_none << ' ';
                elem->formatImpl(settings, state, override_frame);
            }
        };
        format_storage_elem(storage->partition_by, "PARTITION BY");
        format_storage_elem(storage->primary_key, "PRIMARY KEY");
        format_storage_elem(storage->order_by, "ORDER BY");
        format_storage_elem(storage->sample_by, "SAMPLE BY");
        format_storage_elem(storage->ttl_table, "TTL");
    }

    settings.ostr << nl_or_nothing << ')';
}

void ASTTableOverride::applyToCreateTableQuery(ASTCreateQuery * create_query) const
{
    if (columns)
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
                if (exists == dest_children.end())
                    dest_children.emplace_back(override_column_ast);
                else
                    dest_children[exists - dest_children.begin()] = override_column_ast;
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
                    dest_children[exists - dest_children.begin()] = override_index_ast;
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
                    dest_children[exists - dest_children.begin()] = override_constraint_ast;
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
                    dest_children[exists - dest_children.begin()] = override_projection_ast;
            }
        }
    }
    if (storage)
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
        // not supporting overriding ENGINE
    }
}

ASTPtr ASTTableOverrideList::clone() const
{
    auto res = std::make_shared<ASTTableOverrideList>(*this);
    res->cloneChildren();
    return res;
}

ASTPtr ASTTableOverrideList::tryGetTableOverride(const String & name) const
{
    auto it = positions.find(name);
    if (it == positions.end())
        return nullptr;
    return children[it->second];
}

void ASTTableOverrideList::setTableOverride(const String & name, const ASTPtr ast)
{
    auto it = positions.find(name);
    if (it == positions.end())
    {
        positions[name] = children.size();
        children.emplace_back(ast);
    }
    else
    {
        children[it->second] = ast;
    }
}

void ASTTableOverrideList::removeTableOverride(const String & name)
{
    if (hasOverride(name))
    {
        size_t pos = positions[name];
        children.erase(children.begin() + pos);
        positions.erase(name);
        for (auto & pr : positions)
            if (pr.second > pos)
                --pr.second;
    }
}

bool ASTTableOverrideList::hasOverride(const String & name) const
{
    return positions.count(name);
}

void ASTTableOverrideList::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (frame.expression_list_prepend_whitespace)
        settings.ostr << ' ';

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << (settings.one_line ? ", " : ",\n");
        }

        (*it)->formatImpl(settings, state, frame);
    }
}

}
