#include <Parsers/SPARQL/SparqlToSqlTranslator.h>

#include <sstream>
#include <stdexcept>

namespace DB
{
namespace SPARQL
{

void SparqlToSqlTranslator::reset()
{
    alias_counter = 0;
    bindings.clear();
    joins.clear();
}

std::string SparqlToSqlTranslator::newAlias()
{
    return "t" + std::to_string(++alias_counter);
}

std::string SparqlToSqlTranslator::buildSolutionModifiers(const SelectQuery & query)
{
    std::ostringstream out;

    if (!query.order_by.empty())
    {
        out << "\nORDER BY ";
        bool first = true;
        for (const auto & oc : query.order_by)
        {
            if (!first)
                out << ", ";
            first = false;
            if (oc.expr)
                out << translateFilterExpr(*oc.expr);
            if (oc.descending)
                out << " DESC";
        }
    }

    if (query.limit >= 0)
        out << "\nLIMIT " << query.limit;
    if (query.offset >= 0)
        out << "\nOFFSET " << query.offset;

    return out.str();
}

std::string SparqlToSqlTranslator::translate(const SelectQuery & query)
{
    if (!query.where_clause)
        throw std::runtime_error("Missing WHERE clause");

    std::string body;

    if (!query.where_clause->unions.empty())
    {
        std::ostringstream out;
        bool first = true;
        for (const auto & u : query.where_clause->unions)
        {
            if (!first)
                out << "\n";
            first = false;
            out << buildUnionQuery(u, query.projection);
        }
        body = out.str();
    }
    else
    {
        body = translateBranch(*query.where_clause, query.projection, query.distinct);
    }

    std::string modifiers = buildSolutionModifiers(query);
    if (modifiers.empty())
        return body;

    return body + modifiers;
}

void SparqlToSqlTranslator::processTermBinding(
    const Term & term, const std::string & alias, const std::string & column)
{
    if (term.kind == Term::Variable)
    {
        auto it = bindings.find(term.value);
        if (it != bindings.end())
        {
            std::string cond = alias + "." + column + " = " + it->second.alias + "." + it->second.column;
            if (!joins.empty())
                joins.back().on_conditions.push_back(cond);
        }
        else
        {
            bindings[term.value] = {alias, column};
        }
    }
    else
    {
        std::string val;
        if (term.kind == Term::IRI || term.kind == Term::Literal)
            val = "'" + term.value + "'";
        else
            val = term.value;

        std::string cond = alias + "." + column + " = " + val;

        if (!joins.empty())
            joins.back().const_conditions.push_back(cond);
    }
}

void SparqlToSqlTranslator::processTriple(const TriplePattern & tp, bool is_optional)
{
    std::string alias = newAlias();

    JoinEntry entry;
    entry.alias = alias;
    entry.is_left_join = is_optional;
    joins.push_back(std::move(entry));

    processTermBinding(tp.subject, alias, "subject");
    processTermBinding(tp.predicate, alias, "predicate");
    processTermBinding(tp.object, alias, "object");
}

std::string SparqlToSqlTranslator::buildProjection(const std::vector<std::string> & vars, bool distinct)
{
    std::ostringstream out;
    out << "SELECT ";
    if (distinct)
        out << "DISTINCT ";
    bool first = true;
    for (const auto & var : vars)
    {
        if (!first)
            out << ", ";
        first = false;

        auto it = bindings.find(var);
        if (it != bindings.end())
        {
            std::string clean_name = var;
            if (!clean_name.empty() && (clean_name[0] == '?' || clean_name[0] == '$'))
                clean_name = clean_name.substr(1);
            out << it->second.alias << "." << it->second.column << " AS " << clean_name;
        }
        else
        {
            std::string clean_name = var;
            if (!clean_name.empty() && (clean_name[0] == '?' || clean_name[0] == '$'))
                clean_name = clean_name.substr(1);
            out << "NULL AS " << clean_name;
        }
    }
    return out.str();
}

std::string SparqlToSqlTranslator::buildFromAndJoins()
{
    if (joins.empty())
        return "";

    std::ostringstream out;
    out << "\nFROM rdf_triples AS " << joins[0].alias;

    for (size_t i = 1; i < joins.size(); ++i)
    {
        const auto & j = joins[i];
        if (j.is_left_join)
            out << "\nLEFT JOIN rdf_triples AS " << j.alias << " ON ";
        else
            out << "\nJOIN rdf_triples AS " << j.alias << " ON ";

        std::vector<std::string> all_on;
        for (const auto & c : j.on_conditions)
            all_on.push_back(c);

        if (j.is_left_join)
        {
            for (const auto & c : j.const_conditions)
                all_on.push_back(c);
            for (const auto & c : j.filter_conditions)
                all_on.push_back(c);
        }

        if (all_on.empty())
            out << "1 = 1";
        else
        {
            bool first = true;
            for (const auto & c : all_on)
            {
                if (!first)
                    out << " AND ";
                first = false;
                out << c;
            }
        }
    }
    return out.str();
}

std::string SparqlToSqlTranslator::buildPrewhereClause()
{
    if (joins.empty())
        return "";

    const auto & first = joins[0];
    if (first.const_conditions.empty())
        return "";

    std::ostringstream out;
    out << "\nPREWHERE ";
    bool is_first = true;
    for (const auto & c : first.const_conditions)
    {
        if (!is_first)
            out << " AND ";
        is_first = false;
        out << c;
    }
    return out.str();
}

std::string SparqlToSqlTranslator::buildWhereClause()
{
    std::vector<std::string> conditions;

    for (size_t i = 0; i < joins.size(); ++i)
    {
        const auto & j = joins[i];
        if (i == 0)
        {
            for (const auto & c : j.on_conditions)
                conditions.push_back(c);
        }
        else if (!j.is_left_join)
        {
            for (const auto & c : j.const_conditions)
                conditions.push_back(c);
        }
    }

    if (conditions.empty())
        return "";

    std::ostringstream out;
    out << "\nWHERE ";
    bool first = true;
    for (const auto & c : conditions)
    {
        if (!first)
            out << " AND ";
        first = false;
        out << c;
    }
    return out.str();
}

std::string SparqlToSqlTranslator::translateFilterExpr(const FilterExpr & expr)
{
    switch (expr.op)
    {
        case FilterExpr::VarRef:
        {
            auto it = bindings.find(expr.value);
            if (it != bindings.end())
                return it->second.alias + "." + it->second.column;
            return expr.value;
        }
        case FilterExpr::LiteralVal:
        {
            std::string val = expr.value;
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'')
                return val;
            if (val.size() >= 2 && val.front() == '"' && val.back() == '"')
                return "'" + val.substr(1, val.size() - 2) + "'";
            return val;
        }
        case FilterExpr::And:
        {
            std::string result = "(";
            bool first = true;
            for (const auto & child : expr.children)
            {
                if (!first)
                    result += " AND ";
                first = false;
                result += translateFilterExpr(*child);
            }
            return result + ")";
        }
        case FilterExpr::Or:
        {
            std::string result = "(";
            bool first = true;
            for (const auto & child : expr.children)
            {
                if (!first)
                    result += " OR ";
                first = false;
                result += translateFilterExpr(*child);
            }
            return result + ")";
        }
        case FilterExpr::Not:
            return "NOT (" + translateFilterExpr(*expr.children[0]) + ")";

        case FilterExpr::Eq:
            return translateFilterExpr(*expr.children[0]) + " = " + translateFilterExpr(*expr.children[1]);
        case FilterExpr::Ne:
            return translateFilterExpr(*expr.children[0]) + " != " + translateFilterExpr(*expr.children[1]);
        case FilterExpr::Lt:
            return "toInt64OrNull(" + translateFilterExpr(*expr.children[0]) + ") < " + translateFilterExpr(*expr.children[1]);
        case FilterExpr::Gt:
            return "toInt64OrNull(" + translateFilterExpr(*expr.children[0]) + ") > " + translateFilterExpr(*expr.children[1]);
        case FilterExpr::Le:
            return "toInt64OrNull(" + translateFilterExpr(*expr.children[0]) + ") <= " + translateFilterExpr(*expr.children[1]);
        case FilterExpr::Ge:
            return "toInt64OrNull(" + translateFilterExpr(*expr.children[0]) + ") >= " + translateFilterExpr(*expr.children[1]);

        case FilterExpr::Plus:
            return "(" + translateFilterExpr(*expr.children[0]) + " + " + translateFilterExpr(*expr.children[1]) + ")";
        case FilterExpr::Minus:
            return "(" + translateFilterExpr(*expr.children[0]) + " - " + translateFilterExpr(*expr.children[1]) + ")";
        case FilterExpr::Mul:
            return "(" + translateFilterExpr(*expr.children[0]) + " * " + translateFilterExpr(*expr.children[1]) + ")";
        case FilterExpr::Div:
            return "(" + translateFilterExpr(*expr.children[0]) + " / " + translateFilterExpr(*expr.children[1]) + ")";

        case FilterExpr::FnBound:
            return translateFilterExpr(*expr.children[0]) + " IS NOT NULL";
        case FilterExpr::FnIsIRI:
            return "startsWith(" + translateFilterExpr(*expr.children[0]) + ", '<')";
        case FilterExpr::FnIsLiteral:
            return "NOT startsWith(" + translateFilterExpr(*expr.children[0]) + ", '<')";
        case FilterExpr::FnStr:
            return translateFilterExpr(*expr.children[0]);
        case FilterExpr::FnLang:
        case FilterExpr::FnDatatype:
        case FilterExpr::FnIsBlank:
        case FilterExpr::FnSameTerm:
            return "1";

        case FilterExpr::FnLangMatches:
            return translateFilterExpr(*expr.children[0]) + " = " + translateFilterExpr(*expr.children[1]);

        case FilterExpr::FnStrlen:
            return "length(" + translateFilterExpr(*expr.children[0]) + ")";

        case FilterExpr::FnRegex:
        {
            std::string text = translateFilterExpr(*expr.children[0]);
            std::string pattern = translateFilterExpr(*expr.children[1]);
            if (expr.children.size() > 2)
            {
                std::string flags = translateFilterExpr(*expr.children[2]);
                if (flags.find('i') != std::string::npos)
                    return "match(" + text + ", concat('(?i)', " + pattern + "))";
            }
            return "match(" + text + ", " + pattern + ")";
        }
    }
    return "1";
}

std::string SparqlToSqlTranslator::translateBranch(
    const GroupGraphPattern & ggp, const std::vector<std::string> & projection_vars, bool distinct)
{
    reset();

    for (const auto & tp : ggp.triples)
        processTriple(tp, false);

    for (const auto & opt : ggp.optionals)
    {
        if (opt.pattern)
        {
            size_t first_opt_join = joins.size();
            for (const auto & tp : opt.pattern->triples)
                processTriple(tp, true);

            if (!opt.pattern->filters.empty() && first_opt_join < joins.size())
            {
                for (const auto & filter : opt.pattern->filters)
                    joins[first_opt_join].filter_conditions.push_back(translateFilterExpr(*filter));
            }
        }
    }

    std::ostringstream out;
    out << buildProjection(projection_vars, distinct);
    out << buildFromAndJoins();
    out << buildPrewhereClause();

    std::string where = buildWhereClause();
    out << where;

    if (!ggp.filters.empty())
    {
        bool has_where = !where.empty();
        for (const auto & filter : ggp.filters)
        {
            if (!has_where)
            {
                out << "\nWHERE " << translateFilterExpr(*filter);
                has_where = true;
            }
            else
            {
                out << " AND " << translateFilterExpr(*filter);
            }
        }
    }

    return out.str();
}

std::string SparqlToSqlTranslator::buildUnionQuery(
    const Union & u, const std::vector<std::string> & projection_vars)
{
    std::ostringstream out;
    bool first = true;
    for (const auto & alt : u.alternatives)
    {
        if (!first)
            out << "\nUNION ALL\n";
        first = false;
        out << translateBranch(*alt, projection_vars);
    }
    return out.str();
}

}
}
