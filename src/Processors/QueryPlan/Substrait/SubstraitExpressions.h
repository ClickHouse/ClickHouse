#pragma once

#include "config.h"

#if USE_SUBSTRAIT

#include <expected>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Substrait/SubstraitCommon.h>
#include <substrait/algebra.pb.h>

/// How Substrait vocabulary maps to ClickHouse.
/// * Expression. The counterpart of an ActionsDAG node, computed once per row.
/// * Relation (substrait::Rel - ReadRel, FilterRel, ProjectRel). The counterpart of a query plan step.
///   A plan is a tree of relations, each consuming its input relation.
/// * Projection. The relational operation that shapes the columns (select, reorder, compute) and keeps the rows (ProjectRel).
///   Not the MergeTree feature.

namespace DB
{

/// Substrait expressions refer to input columns by position, not name. Creates the expression that reads input column `position`.
substrait::Expression makeColumnReference(size_t position);

/// A complete relation together with the schema of the columns it produces.
struct ConvertedRelation
{
    substrait::Rel rel;
    SubstraitSchema schema;
};

/// Converts ActionsDAG expressions to substrait::Expression over a fixed input schema. Used by the step converters in SubstraitSerializer.
/// When a conversion is not possible, we fail with the blocking reason instead of approximating.
///
/// ClickHouse widens arithmetic results (`plus(Int32, Int32)` -> `Int64`) but Substrait doesn't, so we cast the arguments to CH result type.
/// Unsigned arguments cannot widen for now (https://github.com/substrait-io/substrait/issues/1289).
class SubstraitExpressionConverter
{
public:
    SubstraitExpressionConverter(const SubstraitSchema & input_schema_, SubstraitPlanExtensions & extensions_)
        : input_schema(input_schema_)
        , extensions(extensions_)
    {
    }

    /// One converted expression, the type it emitted, and the DAG node it came from.
    struct ConvertedExpression
    {
        substrait::Expression expression;
        substrait::Type type;
        const ActionsDAG::Node * node = nullptr;
    };

    std::expected<ConvertedExpression, String> convert(const ActionsDAG::Node & node);

    /// Converts the DAG into a ProjectRel that computes `output_header`. The already converted subtree `child` nests inside it as the input.
    /// A DAG that changes nothing, for example a rename, needs no relation, and `child` comes back unchanged.
    std::expected<ConvertedRelation, String> convertProjection(const ActionsDAG & dag, const Block & output_header, substrait::Rel child);

private:
    std::expected<ConvertedExpression, String> convertNodeImpl(const ActionsDAG::Node & node);
    std::expected<ConvertedExpression, String> convertFunction(const ActionsDAG::Node & node);

    /// Brings `arg` to the type named `desired_short_name`. A constant re-encodes as a literal of the target type when its value fits there.
    /// A non-constant gets a Cast, allowed only when lossless (signed int or float widening).
    std::expected<void, String> coerceArgument(ConvertedExpression & arg, std::string_view desired_short_name);

    const SubstraitSchema & input_schema;

    /// The per-plan registry. Conversion adds declarations to it, the plan serializer owns it.
    SubstraitPlanExtensions & extensions;

    /// Without memoization a shared node re-converts once per path.
    std::unordered_map<const ActionsDAG::Node *, ConvertedExpression> converted_nodes;
};

/// Combines converted boolean conditions with `and` into one. A single condition passes through.
SubstraitExpressionConverter::ConvertedExpression
makeConjunction(std::vector<SubstraitExpressionConverter::ConvertedExpression> conjuncts, SubstraitPlanExtensions & extensions);

}

#endif
