#include <Storages/HybridSegmentPruner.h>

#include <Core/Range.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace
{

ASTPtr makeIdentityKeyAST(const Names & column_names)
{
    auto key_ast = make_intrusive<ASTFunction>();
    key_ast->name = "tuple";
    key_ast->arguments = make_intrusive<ASTExpressionList>();
    key_ast->children.push_back(key_ast->arguments);
    for (const auto & name : column_names)
        key_ast->arguments->children.push_back(make_intrusive<ASTIdentifier>(name));
    return key_ast;
}

NamesAndTypesList filterComparable(const NamesAndTypesList & in)
{
    NamesAndTypesList out;
    for (const auto & c : in)
        if (c.type && c.type->isComparable())
            out.push_back(c);
    return out;
}

KeyDescription buildIdentityKey(const NamesAndTypesList & comparable_cols, ContextPtr context)
{
    Names names;
    names.reserve(comparable_cols.size());
    for (const auto & c : comparable_cols)
        names.push_back(c.name);
    return KeyDescription::getKeyFromAST(
        makeIdentityKeyAST(names),
        ColumnsDescription{comparable_cols},
        /*virtuals=*/ {},
        context);
}

NamesAndTypesList namesAndTypesFromKey(const KeyDescription & key)
{
    NamesAndTypesList out;
    for (size_t i = 0; i < key.column_names.size(); ++i)
        out.emplace_back(key.column_names[i], key.data_types[i]);
    return out;
}

}

HybridSegmentPruner::HybridSegmentPruner(
    const ActionsDAGWithInversionPushDown & filter_dag,
    const NamesAndTypesList & hybrid_columns,
    ContextPtr context_)
    : identity_key(buildIdentityKey(filterComparable(hybrid_columns), context_))
    , user_condition(filter_dag, context_,
                     identity_key.column_names, identity_key.expression,
                     /*single_point=*/ false)
    , context(std::move(context_))
{
    useless = identity_key.column_names.empty() || user_condition.alwaysUnknownOrTrue();
}

bool HybridSegmentPruner::canBePruned(const ASTPtr & substituted_segment_predicate) const
{
    if (useless || !substituted_segment_predicate)
        return false;

    auto segment_ast = substituted_segment_predicate->clone();
    auto sample = namesAndTypesFromKey(identity_key);
    auto syntax_result = TreeRewriter(context).analyze(segment_ast, sample);
    auto segment_dag = ExpressionAnalyzer(segment_ast, syntax_result, context).getActionsDAG(true);
    ActionsDAGWithInversionPushDown segment_filter(segment_dag.getOutputs().at(0), context, /* boolean_context */ true);

    KeyCondition segment_condition(
        segment_filter, context,
        identity_key.column_names, identity_key.expression,
        /*single_point=*/ false);

    Hyperrectangle rect;
    rect.reserve(identity_key.column_names.size());

    for (size_t i = 0; i < identity_key.column_names.size(); ++i)
    {
        Ranges col_ranges;
        if (!segment_condition.extractPlainRangesForColumn(i, col_ranges))
        {
            rect.push_back(Range::createWholeUniverse());
            continue;
        }

        if (col_ranges.empty())
            return true;

        if (col_ranges.size() != 1)
        {
            rect.push_back(Range::createWholeUniverse());
            continue;
        }

        rect.push_back(col_ranges.front());
    }

    return !user_condition.checkInHyperrectangle(rect, identity_key.data_types).can_be_true;
}

}
