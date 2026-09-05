#include <Parsers/ASTHypotheticalObjectQuery.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <IO/Operators.h>

namespace DB
{

String ASTHypotheticalObjectQuery::getID(char delim) const
{
    const bool is_projection = object_kind == Projection;
    if (kind == DropAll)
        return is_projection ? "DropAllHypotheticalProjections" : "DropAllHypotheticalIndexes";

    const String prefix = kind == Create ? "CreateHypothetical" : "DropHypothetical";
    return prefix + (is_projection ? "ProjectionQuery" : "IndexQuery") + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTHypotheticalObjectQuery::clone() const
{
    auto res = make_intrusive<ASTHypotheticalObjectQuery>(*this);
    res->children.clear();

    if (object_name)
        res->set(res->object_name, object_name->clone());
    if (index_decl)
        res->set(res->index_decl, index_decl->clone());
    if (projection_decl)
        res->set(res->projection_decl, projection_decl->clone());

    cloneTableOptions(*res);

    return res;
}

void ASTHypotheticalObjectQuery::formatQueryImpl(
    WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    ostr << indent_str;

    const bool is_projection = object_kind == Projection;

    if (kind == DropAll)
    {
        ostr << (is_projection ? "DROP ALL HYPOTHETICAL PROJECTIONS" : "DROP ALL HYPOTHETICAL INDEXES");
        return;
    }

    chassert(object_name);

    if (kind == Create)
        ostr << (is_projection ? "CREATE HYPOTHETICAL PROJECTION " : "CREATE HYPOTHETICAL INDEX ")
             << (if_not_exists ? "IF NOT EXISTS " : "");
    else
        ostr << (is_projection ? "DROP HYPOTHETICAL PROJECTION " : "DROP HYPOTHETICAL INDEX ")
             << (if_exists ? "IF EXISTS " : "");

    object_name->format(ostr, settings, state, frame);
    ostr << " ON ";

    if (table)
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }
        table->format(ostr, settings, state, frame);
    }

    if (kind != Create)
        return;

    if (is_projection)
    {
        /// the name is already printed, so print only the body, not the whole declaration
        chassert(projection_decl);
        projection_decl->as<const ASTProjectionDeclaration &>().formatBody(ostr, settings, state, frame);
        return;
    }

    chassert(index_decl);
    ostr << " ";
    index_decl->format(ostr, settings, state, frame);
}

}
