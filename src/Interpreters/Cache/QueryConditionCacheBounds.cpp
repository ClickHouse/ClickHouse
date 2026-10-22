#include <Interpreters/Cache/QueryConditionCacheBounds.h>

#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/FieldToDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>

#include <base/defines.h>

#include <cmath>

namespace DB
{

namespace
{

/// Like `VirtualColumnUtils::isDeterministic`, this also looks into `COLUMN` nodes holding a
/// constant-folded lambda (`ColumnFunction`): e.g. in `arrayExists(x -> rand() % 2 = 0, arr)` the
/// non-deterministic `rand` lives in the lambda's own `ActionsDAG`, not in this one.
bool isDeterministicSubtree(const ActionsDAG::Node * node)
{
    if (!node->isDeterministic())
        return false;
    if (!allNodeFunctions(*node, [](const IFunctionBase & function) { return function.isDeterministic(); }))
        return false;
    for (const auto * child : node->children)
        if (!isDeterministicSubtree(child))
            return false;
    return true;
}

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

/// Tags separating the parts of the hash of the structure of a condition.
enum class HashTag : UInt8
{
    DeterministicSubtree = 0xD1,
    MonotoneFunction = 0xD2,
    Bound = 0xD4,
};

bool isTimeType(const DataTypePtr & type)
{
    WhichDataType which(removeLowCardinalityAndNullable(type));
    return which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64();
}

/// The literal `100` is a `UInt8` and `300` a `UInt16`, but the comparisons with them differ only in the
/// value, so constants of native numeric types are told apart only by the type of their `Field`.
bool isNativeIntegerOrFloat(const DataTypePtr & type)
{
    WhichDataType which(type);
    return which.isNativeInteger() || which.isFloat();
}

/// A constant, possibly folded from a non-deterministic expression like `now()`.
bool isConstant(const ActionsDAG::Node * node)
{
    return node->column && isColumnConst(*node->column)
        && (node->type == ActionsDAG::ActionType::COLUMN || isDeterministicSubtree(node));
}

/// The kinds of constants a bound may have: a number (including `Decimal` and the date/time types),
/// which can be ordered by its `Field`, or a string compared with an expression of a date/time type
/// (parsed by the comparison), which cannot.
enum class BoundKind : uint8_t
{
    None,
    Ordered,
    Unordered,
};

BoundKind getBoundKind(const DataTypePtr & constant_type, const DataTypePtr & expression_type)
{
    if (constant_type->isNullable() || constant_type->lowCardinality())
        return BoundKind::None;
    WhichDataType which(constant_type);
    if (which.isNativeInteger() || which.isFloat() || which.isDecimal() || isTimeType(constant_type))
        return BoundKind::Ordered;
    if (isStringOrFixedString(constant_type) && isTimeType(expression_type))
        return BoundKind::Unordered;
    return BoundKind::None;
}

String mirrorComparison(const String & name)
{
    if (name == "less")
        return "greater";
    if (name == "lessOrEquals")
        return "greaterOrEquals";
    if (name == "greater")
        return "less";
    return "lessOrEquals";
}

String negateComparison(const String & name)
{
    if (name == "less")
        return "greaterOrEquals";
    if (name == "lessOrEquals")
        return "greater";
    if (name == "greater")
        return "lessOrEquals";
    return "less";
}

struct BoundsAbstractor
{
    ConditionBounds & result;

    /// Returns false if the condition is non-deterministic in another way than by the constants of
    /// the bounds.
    bool visit(const ActionsDAG::Node * node, bool negated, SipHash & hash) const
    {
        const auto * function_node = skipAliases(node);
        if (function_node->type == ActionsDAG::ActionType::FUNCTION)
        {
            const String & name = function_node->function_base->getName();

            if (name == "and" || name == "or" || (name == "not" && function_node->children.size() == 1))
            {
                hash.update(HashTag::MonotoneFunction);
                hash.update(name);
                hash.update(function_node->children.size());
                for (const auto * child : function_node->children)
                    if (!visit(child, negated != (name == "not"), hash))
                        return false;
                return true;
            }

            if ((name == "less" || name == "lessOrEquals" || name == "greater" || name == "greaterOrEquals")
                && function_node->children.size() == 2)
            {
                const auto * lhs = skipAliases(function_node->children[0]);
                const auto * rhs = skipAliases(function_node->children[1]);
                bool lhs_is_constant = isConstant(lhs);
                bool rhs_is_constant = isConstant(rhs);
                if (lhs_is_constant != rhs_is_constant)
                {
                    const auto * constant = lhs_is_constant ? lhs : rhs;
                    const auto * expression = lhs_is_constant ? rhs : lhs;
                    auto kind = getBoundKind(constant->result_type, expression->result_type);
                    Field value = (*constant->column)[0];
                    /// A comparison with NaN is never true, whatever the value of the expression, so the
                    /// condition is not monotone in such a constant, and NaN cannot be ordered either.
                    if (value.getType() == Field::Types::Float64 && std::isnan(value.safeGet<Float64>()))
                        kind = BoundKind::None;
                    /// The value of a non-deterministic constant is all that tells two variants of the
                    /// condition apart, so it has to be ordered: a string is not.
                    if (!isDeterministicSubtree(constant) && kind != BoundKind::Ordered)
                        kind = BoundKind::None;
                    if (kind != BoundKind::None && isDeterministicSubtree(expression))
                    {
                        String function_name = lhs_is_constant ? mirrorComparison(name) : name;

                        hash.update(HashTag::Bound);
                        hash.update(function_name);
                        hash.update(function_node->result_type->getName());
                        if (isNativeIntegerOrFloat(constant->result_type))
                            hash.update(value.getType());
                        else
                            hash.update(constant->result_type->getName());
                        hash.update(HashTag::DeterministicSubtree);
                        expression->updateHash(hash);

                        result.bounds.push_back(ConditionBounds::Bound{
                            .expression = expression,
                            .function_name = std::move(function_name),
                            .type = constant->result_type,
                            .value = std::move(value),
                            .negated = negated,
                        });
                        return true;
                    }
                }
            }
        }

        if (!isDeterministicSubtree(node))
            return false;
        hash.update(HashTag::DeterministicSubtree);
        node->updateHash(hash);
        return true;
    }
};

}

std::vector<Field> ConditionBounds::values() const
{
    std::vector<Field> res;
    res.reserve(bounds.size());
    for (const auto & bound : bounds)
        res.push_back(bound.value);
    return res;
}

std::optional<ConditionBounds> abstractConditionBounds(const ActionsDAG::Node * condition)
{
    if (!condition)
        return std::nullopt;

    ConditionBounds result;
    SipHash hash;
    if (!BoundsAbstractor{result}.visit(condition, /*negated=*/ false, hash) || result.bounds.empty())
        return std::nullopt;

    result.structure_hash = hash.get64();
    return result;
}

bool isDeterministicExceptBounds(const ActionsDAG::Node * condition)
{
    return condition && !isDeterministicSubtree(condition) && abstractConditionBounds(condition).has_value();
}

std::optional<ActionsDAG> buildConditionBoundsBand(
    const ConditionBounds & condition, const std::vector<Field> & earlier_values, const ContextPtr & context)
{
    chassert(condition.bounds.size() == earlier_values.size());

    /// A row is in the band if one of the comparisons admits it with the value that makes the condition
    /// weaker and rejects it with the other value. With the comparison under NOT, the earlier value is
    /// the one that has to admit the row.
    struct Piece
    {
        const ConditionBounds::Bound * bound;
        const Field * admitting;
        const Field * rejecting;
    };
    std::vector<Piece> pieces;
    ActionsDAG::NodeRawConstPtrs expressions;

    for (size_t i = 0; i < condition.bounds.size(); ++i)
    {
        const auto & bound = condition.bounds[i];
        const Field & admitting = bound.negated ? earlier_values[i] : bound.value;
        const Field & rejecting = bound.negated ? bound.value : earlier_values[i];

        if (admitting == rejecting)
            continue;

        if (getBoundKind(bound.type, bound.expression->result_type) == BoundKind::Ordered)
        {
            bool is_upper_bound = bound.function_name == "less" || bound.function_name == "lessOrEquals";
            if (is_upper_bound ? !(rejecting < admitting) : !(admitting < rejecting))
                continue;
        }

        pieces.push_back({&bound, &admitting, &rejecting});
        expressions.push_back(bound.expression);
    }

    if (pieces.empty())
        return std::nullopt;

    ActionsDAG band = ActionsDAG::cloneSubDAG(expressions, /*remove_aliases=*/ false);
    const auto cloned_expressions = band.getOutputs();

    auto add_comparison = [&](const String & function_name, const ActionsDAG::Node * expression, const ConditionBounds::Bound & bound, const Field & value)
    {
        /// The earlier value may not fit into the type of the constant of this variant, see `isNativeIntegerOrFloat`.
        DataTypePtr type = isNativeIntegerOrFloat(bound.type) ? applyVisitor(FieldToDataType(), value) : bound.type;
        ColumnConstPtr column = type->createColumnConst(1, value);
        const auto & constant = band.addColumn(std::move(column), type, applyVisitor(FieldVisitorToString(), value));
        return &band.addFunction(FunctionFactory::instance().get(function_name, context), {expression, &constant}, {});
    };

    auto function_and = FunctionFactory::instance().get("and", context);
    ActionsDAG::NodeRawConstPtrs band_pieces;
    for (size_t i = 0; i < pieces.size(); ++i)
    {
        const auto & piece = pieces[i];
        const auto * admits = add_comparison(piece.bound->function_name, cloned_expressions[i], *piece.bound, *piece.admitting);
        const auto * rejects = add_comparison(negateComparison(piece.bound->function_name), cloned_expressions[i], *piece.bound, *piece.rejecting);
        band_pieces.push_back(&band.addFunction(function_and, {admits, rejects}, {}));
    }

    const auto * output = band_pieces.size() == 1
        ? band_pieces.front()
        : &band.addFunction(FunctionFactory::instance().get("or", context), band_pieces, {});
    band.getOutputs() = {output};
    band.removeUnusedActions();
    return band;
}

}
