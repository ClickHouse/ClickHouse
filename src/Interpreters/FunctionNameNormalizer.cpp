#include <Interpreters/FunctionNameNormalizer.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTTTLElement.h>

#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Poco/String.h>

namespace DB
{

namespace
{

/// True when `canonical_name` re-parses the shape of `node`.
///
/// Some names are routed to a dedicated grammar layer (see `getFunctionLayer` in
/// `ExpressionListParsers.cpp`) while their aliases take the generic function layer, which
/// accepts shapes the dedicated layer rejects. Renaming such an alias turns a parseable AST
/// into one that does not re-parse; the AST is persisted verbatim (view metadata, SQL UDF
/// bodies), so every later read of it throws `SYNTAX_ERROR`. Keep the alias in that case.
///
/// This is an allowlist of provably re-parseable shapes: an unknown or future shape keeps the
/// alias, which always re-parses because it takes the generic layer.
bool canonicalNameCanReparseShape(const String & canonical_name, const ASTFunction & node)
{
    /// `SubstringLayer` accepts `substring(expr, start[, length, ...])` and
    /// `substring(expr FROM start [FOR length])`, but not fewer than two arguments, nor
    /// parameters, nor a window clause, nor a NULLS modifier. The aliases `substr`, `mid` and
    /// `byteSlice` accept all of those.
    if (canonical_name == "substring")
        return node.arguments && node.arguments->children.size() >= 2
            && !node.parameters
            && !node.isWindowFunction() && node.window_name.empty() && !node.window_definition
            && node.getNullsAction() == NullsAction::EMPTY;

    /// Any other alias shares its grammar layer with its canonical name, so renaming cannot
    /// change what re-parses. Add a case here if an alias is registered for another name with
    /// a dedicated layer (`overlay`, `position`, `cast`, `date_part`, ...).
    return true;
}

}

void FunctionNameNormalizer::visit(IAST * ast)
{
    visitImpl(ast, /*for_comparison=*/ false);
}

void FunctionNameNormalizer::visitForComparison(IAST * ast)
{
    visitImpl(ast, /*for_comparison=*/ true);
}

void FunctionNameNormalizer::visitImpl(IAST * ast, bool for_comparison)
{
    if (!ast)
        return;

    // Normalize only selected children. Avoid normalizing engine clause because some engine might
    // have the same name as function, e.g. Log.
    if (auto * node_storage = ast->as<ASTStorage>())
    {
        visitImpl(node_storage->partition_by, for_comparison);
        visitImpl(node_storage->primary_key, for_comparison);
        visitImpl(node_storage->order_by, for_comparison);
        visitImpl(node_storage->sample_by, for_comparison);
        visitImpl(node_storage->ttl_table, for_comparison);
        return;
    }

    // Normalize only selected children. Avoid normalizing type clause because some type might
    // have the same name as function, e.g. Date.
    if (auto * node_decl = ast->as<ASTColumnDeclaration>())
    {
        visitImpl(node_decl->getDefaultExpression().get(), for_comparison);
        visitImpl(node_decl->getTTL().get(), for_comparison);
        return;
    }

    if (auto * node_func = ast->as<ASTFunction>())
    {
        const String & canonical_name
            = getAggregateFunctionCanonicalNameIfAny(getFunctionCanonicalNameIfAny(node_func->name));
        if (canonicalNameCanReparseShape(canonical_name, *node_func))
            node_func->name = canonical_name;
    }

    for (auto & child : ast->children)
        visitImpl(child.get(), for_comparison);

    if (auto * ttl_elem = ast->as<ASTTTLElement>())
    {
        for (const auto & a : ttl_elem->group_by_key)
            visitImpl(a.get(), for_comparison);
        for (const auto & a : ttl_elem->group_by_assignments)
            visitImpl(a.get(), for_comparison);
    }

    /// An `APPLY` transformer carries its function in the non-child `func_name` string, and its
    /// `parameters` and `lambda` are not in `children` either, so the walk above does not reach
    /// them. Stored table definitions are compared as ASTs, so `APPLY SUM` and `APPLY sum` (or a
    /// lambda spelled `x -> SuM(x)`) must compare as the same definition. This only runs on the
    /// comparison path (see `visitForComparison`): the persisted definition keeps the transformer
    /// as written, because older replicas compare the serialized `projections` field
    /// byte-for-byte and the canonical form of an older version is the definition as written.
    if (!for_comparison)
        return;

    /// The engine treats a skip index type case-insensitively (`IndexDescription::getIndexFromAST`
    /// lowercases it), so `TYPE SET(0)` and `TYPE set(0)` are the same index. The stored
    /// definition keeps the type as written.
    if (auto * index_declaration = ast->as<ASTIndexDeclaration>())
    {
        if (auto index_type = index_declaration->getType())
            index_type->name = Poco::toLower(index_type->name);
        return;
    }

    if (auto * apply_transformer = ast->as<ASTColumnsApplyTransformer>())
    {
        if (!apply_transformer->func_name.empty())
        {
            const String & canonical_name = getAggregateFunctionCanonicalNameIfAny(
                getFunctionCanonicalNameIfAny(apply_transformer->func_name));

            /// `APPLY f` expands to a call of `f` with the parameters of the transformer and
            /// exactly one argument, so the shape to check is that of the expansion.
            auto expansion = make_intrusive<ASTFunction>();
            expansion->name = canonical_name;
            expansion->arguments = make_intrusive<ASTExpressionList>();
            expansion->arguments->children.push_back(make_intrusive<ASTIdentifier>("dummy"));
            expansion->parameters = apply_transformer->parameters;
            if (canonicalNameCanReparseShape(canonical_name, *expansion))
                apply_transformer->func_name = canonical_name;
        }

        visitImpl(apply_transformer->parameters.get(), for_comparison);
        visitImpl(apply_transformer->lambda.get(), for_comparison);
    }
}

}
