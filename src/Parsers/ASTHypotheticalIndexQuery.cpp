#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <IO/Operators.h>

namespace DB
{

String ASTHypotheticalIndexQuery::getID(char delim) const
{
    switch (kind)
    {
        case Create:  return "CreateHypotheticalIndexQuery" + (delim + getDatabase()) + delim + getTable();
        case Drop:    return "DropHypotheticalIndexQuery"   + (delim + getDatabase()) + delim + getTable();
        case DropAll: return "DropAllHypotheticalIndexes";
    }
    UNREACHABLE();
}

ASTPtr ASTHypotheticalIndexQuery::clone() const
{
    auto res = make_intrusive<ASTHypotheticalIndexQuery>(*this);
    res->children.clear();

    if (index_name)
    {
        res->index_name = index_name->clone();
        res->children.push_back(res->index_name);
    }

    if (index_decl)
    {
        res->index_decl = index_decl->clone();
        /// `ASTIndexDeclaration::clone` does not carry this flag over; preserve it so the
        /// cloned declaration keeps its `CREATE INDEX` formatting.
        if (auto * cloned_decl = res->index_decl->as<ASTIndexDeclaration>())
            cloned_decl->part_of_create_index_query = index_decl->as<ASTIndexDeclaration &>().part_of_create_index_query;
        res->children.push_back(res->index_decl);
    }

    cloneTableOptions(*res);

    return res;
}

void ASTHypotheticalIndexQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    ostr << indent_str;

    if (kind == DropAll)
    {
        ostr << "DROP ALL HYPOTHETICAL INDEXES";
        return;
    }

    chassert(index_name);

    if (kind == Create)
    {
        ostr << "CREATE HYPOTHETICAL INDEX " << (if_not_exists ? "IF NOT EXISTS " : "");
        index_name->format(ostr, settings, state, frame);
        ostr << " ON ";
    }
    else
    {
        ostr << "DROP HYPOTHETICAL INDEX " << (if_exists ? "IF EXISTS " : "");
        index_name->format(ostr, settings, state, frame);
        ostr << " ON ";
    }

    if (table)
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }
        table->format(ostr, settings, state, frame);
    }

    if (kind == Create)
    {
        chassert(index_decl);
        ostr << " ";
        index_decl->format(ostr, settings, state, frame);
    }
}

}
