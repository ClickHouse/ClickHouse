#include <Parsers/SPARQL/CstToAstBuilder.h>

#include <stdexcept>

namespace DB
{
namespace SPARQL
{

std::unique_ptr<SelectQuery> CstToAstBuilder::build(antlr4_grammars::SparqlParser::QueryContext * query_ctx)
{
    if (auto * prologue = query_ctx->prologue())
        collectPrefixes(prologue);

    auto * select_ctx = query_ctx->selectQuery();
    if (!select_ctx)
        throw std::runtime_error("Only SELECT queries are supported");

    return buildSelectQuery(select_ctx);
}

void CstToAstBuilder::collectPrefixes(antlr4_grammars::SparqlParser::PrologueContext * ctx)
{
    for (auto * decl : ctx->prefixDecl())
    {
        PrefixDecl pd;
        std::string pname = decl->PNAME_NS()->getText();
        if (!pname.empty() && pname.back() == ':')
            pname.pop_back();
        pd.prefix = pname;

        std::string iri = decl->IRI_REF()->getText();
        if (iri.size() >= 2 && iri.front() == '<' && iri.back() == '>')
            iri = iri.substr(1, iri.size() - 2);
        pd.uri = iri;

        prefixes_.push_back(std::move(pd));
    }
}

std::string CstToAstBuilder::expandPrefixedName(const std::string & text) const
{
    auto colon = text.find(':');
    if (colon == std::string::npos)
        return text;

    std::string prefix = text.substr(0, colon);
    std::string local = text.substr(colon + 1);

    for (const auto & pd : prefixes_)
    {
        if (pd.prefix == prefix)
            return pd.uri + local;
    }
    return text;
}

std::unique_ptr<SelectQuery> CstToAstBuilder::buildSelectQuery(antlr4_grammars::SparqlParser::SelectQueryContext * ctx)
{
    auto query = std::make_unique<SelectQuery>();
    query->prefixes = prefixes_;

    for (auto * var : ctx->var_())
        query->projection.push_back(var->getText());

    auto * where_ctx = ctx->whereClause();
    if (!where_ctx || !where_ctx->groupGraphPattern())
        throw std::runtime_error("Missing WHERE clause");

    query->where_clause = buildGroupGraphPattern(where_ctx->groupGraphPattern());
    return query;
}

GroupGraphPatternPtr CstToAstBuilder::buildGroupGraphPattern(antlr4_grammars::SparqlParser::GroupGraphPatternContext * ctx)
{
    auto ggp = std::make_unique<GroupGraphPattern>();

    for (auto * block : ctx->triplesBlock())
    {
        auto * current = block;
        while (current)
        {
            if (auto * tss = current->triplesSameSubject())
            {
                auto patterns = buildTriplePatterns(tss);
                for (auto & tp : patterns)
                    ggp->triples.push_back(std::move(tp));
            }
            current = current->triplesBlock();
        }
    }

    for (auto * gp_not_triples : ctx->graphPatternNotTriples())
    {
        if (auto * opt = gp_not_triples->optionalGraphPattern())
        {
            Optional optional;
            optional.pattern = buildGroupGraphPattern(opt->groupGraphPattern());
            ggp->optionals.push_back(std::move(optional));
        }
        else if (auto * union_ctx = gp_not_triples->groupOrUnionGraphPattern())
        {
            auto groups = union_ctx->groupGraphPattern();
            if (groups.size() > 1)
            {
                Union u;
                for (auto * g : groups)
                    u.alternatives.push_back(buildGroupGraphPattern(g));
                ggp->unions.push_back(std::move(u));
            }
            else if (groups.size() == 1)
            {
                auto nested = buildGroupGraphPattern(groups[0]);
                for (auto & t : nested->triples)
                    ggp->triples.push_back(std::move(t));
                for (auto & o : nested->optionals)
                    ggp->optionals.push_back(std::move(o));
                for (auto & u : nested->unions)
                    ggp->unions.push_back(std::move(u));
                for (auto & f : nested->filters)
                    ggp->filters.push_back(std::move(f));
            }
        }
    }

    for (auto * filter_ctx : ctx->filter_())
    {
        auto * constraint = filter_ctx->constraint();
        if (auto * bracketted = constraint->brackettedExpression())
            ggp->filters.push_back(buildFilterExpr(bracketted->expression()));
        else if (auto * builtin = constraint->builtInCall())
            ggp->filters.push_back(buildBuiltInCall(builtin));
    }

    return ggp;
}

std::vector<TriplePattern> CstToAstBuilder::buildTriplePatterns(antlr4_grammars::SparqlParser::TriplesSameSubjectContext * ctx)
{
    std::vector<TriplePattern> result;

    auto * varOrTerm = ctx->varOrTerm();
    if (!varOrTerm)
        throw std::runtime_error("Expected subject in triple pattern");

    Term subject = buildTerm(varOrTerm);

    auto * propList = ctx->propertyListNotEmpty();
    if (!propList)
        throw std::runtime_error("Expected property list in triple pattern");

    auto verbs = propList->verb();
    auto objectLists = propList->objectList();

    for (size_t i = 0; i < verbs.size() && i < objectLists.size(); ++i)
    {
        Term predicate;
        auto * verb = verbs[i];
        if (verb->A())
        {
            predicate = Term{Term::IRI, "rdf:type", "", ""};
        }
        else if (auto * varOrIRI = verb->varOrIRIref())
        {
            if (auto * v = varOrIRI->var_())
                predicate = buildTermFromVar(v);
            else if (auto * iri = varOrIRI->iriRef())
                predicate = buildTermFromIRI(iri);
        }

        auto * objList = objectLists[i];
        for (auto * obj : objList->object_())
        {
            auto * graphNode = obj->graphNode();
            if (auto * vot = graphNode->varOrTerm())
            {
                TriplePattern tp;
                tp.subject = subject;
                tp.predicate = predicate;
                tp.object = buildTerm(vot);
                result.push_back(std::move(tp));
            }
        }
    }

    return result;
}

Term CstToAstBuilder::buildTerm(antlr4_grammars::SparqlParser::VarOrTermContext * ctx)
{
    if (auto * v = ctx->var_())
        return buildTermFromVar(v);
    if (auto * gt = ctx->graphTerm())
    {
        if (auto * iri = gt->iriRef())
            return buildTermFromIRI(iri);
        if (auto * lit = gt->rdfLiteral())
            return buildTermFromLiteral(lit);
        if (auto * num = gt->numericLiteral())
            return buildTermFromNumeric(num);
        if (gt->booleanLiteral())
            return Term{Term::Literal, gt->booleanLiteral()->getText(), "xsd:boolean", ""};
    }
    throw std::runtime_error("Unsupported term: " + ctx->getText());
}

Term CstToAstBuilder::buildTermFromVar(antlr4_grammars::SparqlParser::Var_Context * ctx)
{
    return Term{Term::Variable, ctx->getText(), "", ""};
}

Term CstToAstBuilder::buildTermFromIRI(antlr4_grammars::SparqlParser::IriRefContext * ctx)
{
    std::string text = ctx->getText();
    if (text.size() >= 2 && text.front() == '<' && text.back() == '>')
        text = text.substr(1, text.size() - 2);
    else
        text = expandPrefixedName(text);
    return Term{Term::IRI, text, "", ""};
}

Term CstToAstBuilder::buildTermFromLiteral(antlr4_grammars::SparqlParser::RdfLiteralContext * ctx)
{
    Term t;
    t.kind = Term::Literal;
    std::string raw = ctx->string_()->getText();
    if (raw.size() >= 2)
        t.value = raw.substr(1, raw.size() - 2);
    else
        t.value = raw;

    if (ctx->LANGTAG())
    {
        std::string lang = ctx->LANGTAG()->getText();
        if (!lang.empty() && lang[0] == '@')
            lang = lang.substr(1);
        t.lang = lang;
    }
    if (ctx->iriRef())
        t.datatype = buildTermFromIRI(ctx->iriRef()).value;

    return t;
}

Term CstToAstBuilder::buildTermFromNumeric(antlr4_grammars::SparqlParser::NumericLiteralContext * ctx)
{
    return Term{Term::Literal, ctx->getText(), "xsd:numeric", ""};
}

FilterExprPtr CstToAstBuilder::buildFilterExpr(antlr4_grammars::SparqlParser::ExpressionContext * ctx)
{
    return buildConditionalOr(ctx->conditionalOrExpression());
}

FilterExprPtr CstToAstBuilder::buildConditionalOr(antlr4_grammars::SparqlParser::ConditionalOrExpressionContext * ctx)
{
    auto parts = ctx->conditionalAndExpression();
    if (parts.size() == 1)
        return buildConditionalAnd(parts[0]);

    auto node = std::make_unique<FilterExpr>();
    node->op = FilterExpr::Or;
    for (auto * p : parts)
        node->children.push_back(buildConditionalAnd(p));
    return node;
}

FilterExprPtr CstToAstBuilder::buildConditionalAnd(antlr4_grammars::SparqlParser::ConditionalAndExpressionContext * ctx)
{
    auto parts = ctx->valueLogical();
    if (parts.size() == 1)
        return buildRelational(parts[0]->relationalExpression());

    auto node = std::make_unique<FilterExpr>();
    node->op = FilterExpr::And;
    for (auto * p : parts)
        node->children.push_back(buildRelational(p->relationalExpression()));
    return node;
}

FilterExprPtr CstToAstBuilder::buildRelational(antlr4_grammars::SparqlParser::RelationalExpressionContext * ctx)
{
    auto numeric_parts = ctx->numericExpression();
    if (numeric_parts.size() == 1)
        return buildAdditive(numeric_parts[0]->additiveExpression());

    auto node = std::make_unique<FilterExpr>();
    if (ctx->EQUAL()) node->op = FilterExpr::Eq;
    else if (ctx->NOT_EQUAL()) node->op = FilterExpr::Ne;
    else if (ctx->LESS()) node->op = FilterExpr::Lt;
    else if (ctx->GREATER()) node->op = FilterExpr::Gt;
    else if (ctx->LESS_OR_EQUAL()) node->op = FilterExpr::Le;
    else if (ctx->GREATER_OR_EQUAL()) node->op = FilterExpr::Ge;
    else throw std::runtime_error("Unknown relational operator");

    node->children.push_back(buildAdditive(numeric_parts[0]->additiveExpression()));
    node->children.push_back(buildAdditive(numeric_parts[1]->additiveExpression()));
    return node;
}

FilterExprPtr CstToAstBuilder::buildAdditive(antlr4_grammars::SparqlParser::AdditiveExpressionContext * ctx)
{
    auto parts = ctx->multiplicativeExpression();
    if (parts.size() == 1)
        return buildMultiplicative(parts[0]);

    auto result = buildMultiplicative(parts[0]);
    for (size_t i = 1; i < parts.size(); ++i)
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::Plus;
        node->children.push_back(std::move(result));
        node->children.push_back(buildMultiplicative(parts[i]));
        result = std::move(node);
    }
    return result;
}

FilterExprPtr CstToAstBuilder::buildMultiplicative(antlr4_grammars::SparqlParser::MultiplicativeExpressionContext * ctx)
{
    auto parts = ctx->unaryExpression();
    if (parts.size() == 1)
        return buildUnary(parts[0]);

    auto result = buildUnary(parts[0]);
    for (size_t i = 1; i < parts.size(); ++i)
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::Mul;
        node->children.push_back(std::move(result));
        node->children.push_back(buildUnary(parts[i]));
        result = std::move(node);
    }
    return result;
}

FilterExprPtr CstToAstBuilder::buildUnary(antlr4_grammars::SparqlParser::UnaryExpressionContext * ctx)
{
    if (ctx->EXCLAMATION())
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::Not;
        node->children.push_back(buildPrimary(ctx->primaryExpression()));
        return node;
    }
    if (ctx->MINUS())
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::Minus;
        auto zero = std::make_unique<FilterExpr>();
        zero->op = FilterExpr::LiteralVal;
        zero->value = "0";
        node->children.push_back(std::move(zero));
        node->children.push_back(buildPrimary(ctx->primaryExpression()));
        return node;
    }
    return buildPrimary(ctx->primaryExpression());
}

FilterExprPtr CstToAstBuilder::buildPrimary(antlr4_grammars::SparqlParser::PrimaryExpressionContext * ctx)
{
    if (auto * bracket = ctx->brackettedExpression())
        return buildFilterExpr(bracket->expression());

    if (auto * builtin = ctx->builtInCall())
        return buildBuiltInCall(builtin);

    if (auto * var = ctx->var_())
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::VarRef;
        node->value = var->getText();
        return node;
    }

    if (ctx->numericLiteral() || ctx->rdfLiteral() || ctx->booleanLiteral())
    {
        auto node = std::make_unique<FilterExpr>();
        node->op = FilterExpr::LiteralVal;
        node->value = ctx->getText();
        return node;
    }

    throw std::runtime_error("Unsupported primary expression: " + ctx->getText());
}

FilterExprPtr CstToAstBuilder::buildBuiltInCall(antlr4_grammars::SparqlParser::BuiltInCallContext * ctx)
{
    auto node = std::make_unique<FilterExpr>();

    if (ctx->STR()) node->op = FilterExpr::FnStr;
    else if (ctx->LANG()) node->op = FilterExpr::FnLang;
    else if (ctx->DATATYPE()) node->op = FilterExpr::FnDatatype;
    else if (ctx->BOUND()) node->op = FilterExpr::FnBound;
    else if (ctx->IS_IRI() || ctx->IS_URI()) node->op = FilterExpr::FnIsIRI;
    else if (ctx->IS_BLANK()) node->op = FilterExpr::FnIsBlank;
    else if (ctx->IS_LITERAL()) node->op = FilterExpr::FnIsLiteral;
    else if (ctx->SAME_TERM()) node->op = FilterExpr::FnSameTerm;
    else if (ctx->LANGMATCHES()) node->op = FilterExpr::FnLangMatches;
    else if (ctx->regexExpression()) node->op = FilterExpr::FnRegex;
    else throw std::runtime_error("Unsupported built-in call: " + ctx->getText());

    if (ctx->regexExpression())
    {
        for (auto * expr : ctx->regexExpression()->expression())
            node->children.push_back(buildFilterExpr(expr));
    }
    else
    {
        for (auto * expr : ctx->expression())
            node->children.push_back(buildFilterExpr(expr));

        if (ctx->var_())
        {
            auto var_node = std::make_unique<FilterExpr>();
            var_node->op = FilterExpr::VarRef;
            var_node->value = ctx->var_()->getText();
            node->children.push_back(std::move(var_node));
        }
    }

    return node;
}

}
}
