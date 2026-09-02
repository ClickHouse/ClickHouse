#include <Interpreters/Cache/QueryConditionCacheTimeConditions.h>

#include <Columns/ColumnConst.h>
#include <Common/DateLUTImpl.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/IFunction.h>

#include <base/arithmeticOverflow.h>
#include <base/defines.h>
#include <fmt/format.h>

#include <cmath>
#include <limits>

namespace DB
{

namespace
{

/// Grid steps in seconds, largest first. All steps divide one day, so values that are aligned to
/// a day (or to a coarser step) stay aligned when rounded to a finer step.
constexpr std::array<Int64, 7> grid_ladder = {86400, 21600, 3600, 600, 60, 10, 1};

/// Choose the grid step for a constant: the largest ladder step not exceeding the distance between
/// the constant and the current time, multiplied by the configured factor and capped at one day.
/// This bounds the rounding error relative to the time window the query looks at, and makes the
/// derived boundary (and hence the cache key) rotate roughly once per the same fraction of the window.
std::optional<Int64> chooseGridStep(Int64 constant_seconds, double grid_factor, time_t current_time)
{
    double distance = std::abs(static_cast<double>(current_time) - static_cast<double>(constant_seconds));
    distance = std::max(distance, 1.0);
    double target = distance * grid_factor;
    if (!(target >= 1.0)) /// Also rejects NaN.
        return std::nullopt;
    target = std::min(target, static_cast<double>(grid_ladder.front()));
    for (Int64 step : grid_ladder)
        if (static_cast<double>(step) <= target)
            return step;
    return std::nullopt;
}

/// Round down to the previous grid point. Grid points for sub-day steps are aligned to the local
/// (DST-aware) day in the timezone of the constant's data type, so constants produced by functions
/// like toStartOfDay() or toStartOfHour() land exactly on a grid point and survive the rounding
/// unchanged. The final min() guards the floor property against LUT range clamping - soundness of
/// the derived condition depends on it, alignment is just a matter of cache hit rate.
Int64 floorToGridStep(Int64 t, Int64 step, const DateLUTImpl & lut)
{
    Int64 res = t;
    switch (step)
    {
        case 1:
            return t;
        case 10:
            res = lut.toStartOfSecondInterval(t, 10);
            break;
        case 60:
            res = lut.toStartOfMinuteInterval(t, 1);
            break;
        case 600:
            res = lut.toStartOfMinuteInterval(t, 10);
            break;
        case 3600:
            res = lut.toStartOfHourInterval(t, 1);
            break;
        case 21600:
            res = lut.toStartOfHourInterval(t, 6);
            break;
        case 86400:
            res = lut.toDate(t);
            break;
        default:
            chassert(false);
    }
    return std::min(res, t);
}

/// Round up to the next grid point. Implemented on top of the floor: step forward by the nominal
/// step size and round down, with fallbacks for timezones where local grid intervals can be longer
/// than the nominal step (DST transitions). The result is always >= t; exact alignment is only a
/// matter of cache hit rate.
Int64 ceilToGridStep(Int64 t, Int64 step, const DateLUTImpl & lut)
{
    Int64 res = floorToGridStep(t, step, lut);
    if (res == t)
        return t;
    res = floorToGridStep(t + step, step, lut);
    if (res <= t)
        res = floorToGridStep(t + 2 * step, step, lut);
    if (res <= t)
        res = t + step;
    return res;
}

/// The value of the constant expressed in seconds, for choosing the grid step.
std::optional<Int64> constantValueInSeconds(const IDataType & type, const Field & value)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Date:
            return static_cast<Int64>(value.safeGet<UInt64>()) * 86400;
        case TypeIndex::Date32:
            return value.safeGet<Int64>() * 86400;
        case TypeIndex::DateTime:
            return static_cast<Int64>(value.safeGet<UInt64>());
        case TypeIndex::DateTime64:
        {
            const auto & decimal = value.safeGet<DecimalField<DateTime64>>();
            Int64 ticks = decimal.getValue().value;
            if (ticks == std::numeric_limits<Int64>::min())
                return std::nullopt;
            Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(decimal.getScale());
            /// Floor division.
            return ticks >= 0 ? ticks / multiplier : -((-ticks + multiplier - 1) / multiplier);
        }
        default:
            return std::nullopt;
    }
}

/// Round a constant of a date/time type onto the grid. Returns std::nullopt if the type is not a
/// supported date/time type or no valid grid step exists.
std::optional<Field> roundTimeConstant(const IDataType & type, const Field & value, bool round_up, double grid_factor, time_t current_time)
{
    std::optional<Int64> seconds = constantValueInSeconds(type, value);
    if (!seconds)
        return std::nullopt;

    /// Keep all further arithmetic (stepping by up to two days, converting back to sub-second
    /// ticks) trivially free of overflow. Real timestamps are nowhere near this bound.
    constexpr Int64 max_reasonable_seconds = 1'000'000'000'000'000; /// ~year 31 million
    if (*seconds > max_reasonable_seconds || *seconds < -max_reasonable_seconds)
        return std::nullopt;

    std::optional<Int64> step = chooseGridStep(*seconds, grid_factor, current_time);
    if (!step)
        return std::nullopt;

    switch (type.getTypeId())
    {
        case TypeIndex::Date:
        case TypeIndex::Date32:
        {
            /// Date values are whole days and every grid step divides one day, so the rounding is
            /// the identity: the derived condition merely relabels the constant as deterministic.
            return value;
        }
        case TypeIndex::DateTime:
        {
            const auto & lut = assert_cast<const DataTypeDateTime &>(type).getTimeZone();
            Int64 t = *seconds;
            Int64 rounded = round_up ? ceilToGridStep(t, *step, lut) : floorToGridStep(t, *step, lut);
            rounded = std::clamp<Int64>(rounded, 0, std::numeric_limits<UInt32>::max());
            /// The clamping cannot break the rounding direction because t itself is in the range.
            return Field(static_cast<UInt64>(rounded));
        }
        case TypeIndex::DateTime64:
        {
            const auto & datetime64 = assert_cast<const DataTypeDateTime64 &>(type);
            const auto & lut = datetime64.getTimeZone();
            const auto & decimal = value.safeGet<DecimalField<DateTime64>>();
            UInt32 scale = decimal.getScale();
            Int64 multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);
            Int64 ticks = decimal.getValue().value;
            Int64 whole_seconds = *seconds;
            Int64 fraction = ticks - whole_seconds * multiplier; /// In [0, multiplier).

            Int64 rounded_seconds = 0;
            if (round_up)
                rounded_seconds = ceilToGridStep(fraction == 0 ? whole_seconds : whole_seconds + 1, *step, lut);
            else
                rounded_seconds = floorToGridStep(whole_seconds, *step, lut);

            Int64 rounded_ticks = 0;
            if (common::mulOverflow(rounded_seconds, multiplier, rounded_ticks))
                return std::nullopt;
            return Field(DecimalField<DateTime64>(DateTime64(rounded_ticks), scale));
        }
        default:
            return std::nullopt;
    }
}

bool isTopKFilterFunction(const ActionsDAG::Node * node)
{
    return node->type == ActionsDAG::ActionType::FUNCTION
        && node->function_base
        && node->function_base->getName() == "__topKFilter";
}

/// `allow_top_k_filter` treats the internal `__topKFilter` function as an opaque deterministic leaf,
/// mirroring `isDeterministicAllowingTopKFilter` in `updateQueryConditionCache.cpp` and
/// `ReadFromMergeTree.cpp`: TopK dynamic filtering folds `__topKFilter` into the storage filter DAG
/// as `and(__topKFilter(...), <predicate>)`, and the write and read sides already partition the
/// cache key by the TopK plan parameters. Without this, a TopK read of a current-time condition
/// would derive nothing at all and bypass the cache entirely.
bool isDeterministicSubtree(const ActionsDAG::Node * node, bool allow_top_k_filter)
{
    if (!node->isDeterministic() && !(allow_top_k_filter && isTopKFilterFunction(node)))
        return false;
    for (const auto * child : node->children)
        if (!isDeterministicSubtree(child, allow_top_k_filter))
            return false;
    return true;
}

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

bool isRoundableConstant(const ActionsDAG::Node * node)
{
    return node->type == ActionsDAG::ActionType::COLUMN
        && !node->isDeterministic()
        && node->column
        && isColumnConst(*node->column);
}

/// The derived condition is hashed with a scheme of its own rather than materialized as an
/// ActionsDAG: deterministic subtrees are hashed exactly as ActionsDAG::Node::updateHash does,
/// while the rewritten spine (AND/OR/NOT/comparisons) and the rounded constants are hashed by
/// structure and value under distinct tags. All that soundness requires is that the write side and
/// the read side of the cache derive identical hashes from semantically related conditions, and
/// both use this same routine. The tags keep derived hashes from colliding with hashes of ordinary
/// deterministic conditions (those are plain ActionsDAG::Node::getHash values).
enum class HashTag : UInt8
{
    DeterministicSubtree = 0xD1,
    MonotoneFunction = 0xD2,
    RoundedConstant = 0xD3,
};

struct Rewriter
{
    double grid_factor;
    time_t current_time;
    bool allow_top_k_filter;

    /// Hash the condition rewritten with rounded time constants into `hash` and render it into
    /// `description`. `weaken` gives the current rounding direction; it flips under NOT.
    /// Returns false if the condition contains non-determinism that cannot be rounded away.
    bool hashRewritten(const ActionsDAG::Node * node, bool weaken, SipHash & hash, String & description) const
    {
        if (isDeterministicSubtree(node, allow_top_k_filter))
        {
            hash.update(HashTag::DeterministicSubtree);
            node->updateHash(hash);
            description += node->result_name;
            return true;
        }

        node = skipAliases(node);

        if (node->type != ActionsDAG::ActionType::FUNCTION)
            return false;

        const String & name = node->function_base->getName();

        if (name == "and" || name == "or" || name == "not")
        {
            if (name == "not" && node->children.size() != 1)
                return false;

            hash.update(HashTag::MonotoneFunction);
            hash.update(name);
            hash.update(node->children.size());
            description += name;
            description += '(';
            bool first = true;
            for (const auto * child : node->children)
            {
                if (!first)
                    description += ", ";
                first = false;
                /// NOT is antitone: a weaker argument makes the negation stronger and vice versa.
                if (!hashRewritten(child, name == "not" ? !weaken : weaken, hash, description))
                    return false;
            }
            description += ')';
            return true;
        }

        bool is_less = (name == "less" || name == "lessOrEquals");
        bool is_greater = (name == "greater" || name == "greaterOrEquals");
        if ((is_less || is_greater) && node->children.size() == 2)
        {
            const auto * lhs = skipAliases(node->children[0]);
            const auto * rhs = skipAliases(node->children[1]);

            bool lhs_is_constant = isRoundableConstant(lhs);
            bool rhs_is_constant = isRoundableConstant(rhs);

            /// Exactly one side must be a non-deterministic constant and the other side must be
            /// deterministic; otherwise the comparison cannot be rounded soundly.
            if (lhs_is_constant == rhs_is_constant)
                return false;
            const auto * constant = lhs_is_constant ? lhs : rhs;
            const auto * other = lhs_is_constant ? rhs : lhs;
            /// Strict here: the compared expression must not hide a `__topKFilter` (its value is
            /// not a fixed quantity a rounded bound could be compared against monotonically).
            if (!isDeterministicSubtree(other, /*allow_top_k_filter=*/false))
                return false;

            /// The constant is an upper bound on the deterministic side for `expr < K` and for
            /// `K > expr`, and a lower bound for `expr > K` and `K < expr`. Weakening moves an
            /// upper bound up and a lower bound down; strengthening does the opposite.
            bool constant_is_upper_bound = (rhs_is_constant == is_less);
            bool round_up = (weaken == constant_is_upper_bound);

            Field constant_value = (*constant->column)[0];
            std::optional<Field> rounded
                = roundTimeConstant(*constant->result_type, constant_value, round_up, grid_factor, current_time);
            if (!rounded)
                return false;

            String rounded_description = fmt::format(
                "_rounded({}, {})", applyVisitor(FieldVisitorToString(), *rounded), constant->result_type->getName());

            hash.update(HashTag::MonotoneFunction);
            hash.update(name);
            hash.update(node->children.size());
            description += name;
            description += '(';
            for (size_t i = 0; i < 2; ++i)
            {
                if (i == 1)
                    description += ", ";
                const auto * child = (i == 0) ? lhs : rhs;
                if (child == constant)
                {
                    hash.update(HashTag::RoundedConstant);
                    hash.update(constant->result_type->getName());
                    hash.update(rounded_description);
                    description += rounded_description;
                }
                else
                {
                    if (!hashRewritten(child, weaken, hash, description))
                        return false;
                }
            }
            description += ')';
            return true;
        }

        return false;
    }
};

}

std::optional<DeterministicTimeCondition> deriveDeterministicTimeCondition(
    const ActionsDAG::Node * condition,
    TimeConditionRounding rounding,
    double grid_factor,
    time_t current_time,
    bool allow_top_k_filter)
{
    if (!condition || grid_factor <= 0)
        return std::nullopt;

    /// An already deterministic condition needs no derivation; keep its ordinary hash as the cache
    /// key so this feature does not affect existing conditions in any way.
    if (isDeterministicSubtree(condition, allow_top_k_filter))
        return std::nullopt;

    SipHash hash;
    String description;
    Rewriter rewriter{grid_factor, current_time, allow_top_k_filter};
    if (!rewriter.hashRewritten(condition, rounding == TimeConditionRounding::Weaken, hash, description))
        return std::nullopt;

    return DeterministicTimeCondition{hash.get64(), description};
}

}
