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

    if (is_standalone)
    {
        settings.ostr << hl_keyword << "TABLE OVERRIDE " << hl_none;
        ASTIdentifier(table_name).formatImpl(settings, state, frame);
    }
    if (!columns && (!storage || storage->children.empty()))
        return;
    auto override_frame = frame;
    if (is_standalone)
    {
        ++override_frame.indent;
        settings.ostr << nl_or_ws << '(' << nl_or_nothing;
    }
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

    if (is_standalone)
        settings.ostr << nl_or_nothing << ')';
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

void ASTTableOverrideList::setTableOverride(const String & name, ASTPtr ast)
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
    return positions.contains(name);
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
