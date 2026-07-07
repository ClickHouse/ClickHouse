#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


String ASTDropQuery::getID(char delim) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        return "DropQuery" + (delim + getDatabase()) + delim + getTable();
    if (kind == ASTDropQuery::Kind::Detach)
        return "DetachQuery" + (delim + getDatabase()) + delim + getTable();
    if (kind == ASTDropQuery::Kind::Truncate)
        return "TruncateQuery" + (delim + getDatabase()) + delim + getTable();
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");
}

ASTPtr ASTDropQuery::clone() const
{
    auto res = make_intrusive<ASTDropQuery>(*this);
    cloneOutputOptions(*res);
    cloneTableOptions(*res);
    return res;
}

void ASTDropQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic flags and selectors that decide what is dropped and how, and that live
    /// outside `children` (the `database` / `table` identifiers and the `database_and_tables` list
    /// are part of `children` and are hashed through the child recursion, so they are not repeated
    /// here). See the header comment for why the rewrite-rule matcher needs this. Each field is
    /// produced by the formatter, so it survives the format -> parse round-trip that the debug-build
    /// AST consistency check requires -- or is never set by the parser (`no_ddl_lock`).
    hash_state.update(kind);
    hash_state.update(if_exists);
    hash_state.update(if_empty);
    hash_state.update(no_ddl_lock);
    hash_state.update(has_all);
    hash_state.update(has_tables);
    hash_state.update(like);
    hash_state.update(not_like);
    hash_state.update(case_insensitive_like);
    hash_state.update(is_dictionary);
    hash_state.update(is_view);
    hash_state.update(sync);
    hash_state.update(permanently);
    hash_state.update(isTemporary());
    hash_state.update(cluster);
}

void ASTDropQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (kind == ASTDropQuery::Kind::Drop)
        ostr << "DROP ";
    else if (kind == ASTDropQuery::Kind::Detach)
        ostr << "DETACH ";
    else if (kind == ASTDropQuery::Kind::Truncate)
        ostr << "TRUNCATE ";
    else
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Not supported kind of drop query.");

    if (isTemporary())
        ostr << "TEMPORARY ";

    if (has_all)
        ostr << "ALL ";
    if (has_tables)
        ostr << "TABLES FROM ";
    else if (!table && !database_and_tables && database)
        ostr << "DATABASE ";
    else if (is_dictionary)
        ostr << "DICTIONARY ";
    else if (is_view)
        ostr << "VIEW ";
    else
        ostr << "TABLE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    if (if_empty)
        ostr << "IF EMPTY ";

    if (!table && !database_and_tables && database)
    {
        database->format(ostr, settings, state, frame);
    }
    else if (database_and_tables)
    {
        auto & list = database_and_tables->as<ASTExpressionList &>();
        for (auto it = list.children.begin(); it != list.children.end(); ++it)
        {
            if (it != list.children.begin())
                ostr << ", ";

            auto identifier = dynamic_pointer_cast<ASTTableIdentifier>(*it);
            if (!identifier)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

            if (auto db = identifier->getDatabase())
            {
                db->format(ostr, settings, state, frame);
                ostr << '.';
            }

            auto tb = identifier->getTable();
            chassert(tb);
            tb->format(ostr, settings, state, frame);
        }
    }
    else
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }

    /// Emit the clause whenever a LIKE was present, even for an empty pattern, so that the
    /// `not_like` / `case_insensitive_like` flags (now folded into the tree hash) survive the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    if (!like.empty() || not_like || case_insensitive_like)
    {
        ostr
            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << quoteString(like);
    }

    formatOnCluster(ostr, settings);

    if (permanently)
        ostr << " PERMANENTLY";

    if (sync)
        ostr << " SYNC";
}

ASTs ASTDropQuery::getRewrittenASTsOfSingleTable(ASTPtr self) const
{
    ASTs res;
    if (database_and_tables == nullptr)
    {
        res.push_back(self);
        return res;
    }

    auto & list = database_and_tables->as<ASTExpressionList &>();
    for (const auto & child : list.children)
    {
        auto cloned = clone();
        auto & query = cloned->as<ASTDropQuery &>();
        query.database_and_tables = nullptr;
        query.children.clear();

        auto database_and_table = dynamic_pointer_cast<ASTTableIdentifier>(child);
        if (!database_and_table)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

        query.database = database_and_table->getDatabase();
        query.table = database_and_table->getTable();

        if (query.database)
            query.children.push_back(query.database);

        if (query.table)
            query.children.push_back(query.table);

        res.push_back(cloned);
    }
    return res;
}

}
