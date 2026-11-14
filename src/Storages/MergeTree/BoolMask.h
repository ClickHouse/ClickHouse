#pragma once

#include <fmt/format.h>

/// Multiple Boolean values. That is, whether it can be true, whether it can be false, and whether it can be NULL.
/// NULL is neither true nor false, so a condition is true on every row only if it can be neither false nor NULL.
struct BoolMask
{
    bool can_be_true = false;
    bool can_be_false = false;
    bool can_be_null = false;

    BoolMask() = default;
    /// A caller that doesn't track NULL leaves `can_be_null_` false and counts a NULL row in `can_be_false_`.
    BoolMask(bool can_be_true_, bool can_be_false_, bool can_be_null_ = false)
        : can_be_true(can_be_true_), can_be_false(can_be_false_), can_be_null(can_be_null_) { }

    /// The operators follow three-valued logic. A row is NULL under `AND` if one side is NULL and the other is true or
    /// NULL, and under `OR` if one side is NULL and the other is false or NULL.
    BoolMask operator&(const BoolMask & m) const
    {
        return {
            can_be_true && m.can_be_true,
            can_be_false || m.can_be_false,
            (can_be_null && (m.can_be_true || m.can_be_null)) || (m.can_be_null && (can_be_true || can_be_null))};
    }
    BoolMask operator|(const BoolMask & m) const
    {
        return {
            can_be_true || m.can_be_true,
            can_be_false && m.can_be_false,
            (can_be_null && (m.can_be_false || m.can_be_null)) || (m.can_be_null && (can_be_false || can_be_null))};
    }
    BoolMask operator!() const { return {can_be_false, can_be_true, can_be_null}; }

    bool operator==(const BoolMask & other) const
    {
        return can_be_true == other.can_be_true && can_be_false == other.can_be_false && can_be_null == other.can_be_null;
    }

    bool canBeFalseOrNull() const { return can_be_false || can_be_null; }

    /// Check if `can_be_true` and `canBeFalseOrNull()` are no longer changeable under BoolMask::combine.
    /// We use this condition to early-exit KeyConditions::checkInRange methods.
    bool isComplete() const
    {
        return can_be_true && canBeFalseOrNull();
    }

    /// Combine check result in different hyperrectangles.
    static BoolMask combine(const BoolMask & left, const BoolMask & right)
    {
        return {
            left.can_be_true || right.can_be_true,
            left.can_be_false || right.can_be_false,
            left.can_be_null || right.can_be_null};
    }

    /// Combine two checks of the same condition in the same hyperrectangle. A check may report a component
    /// as possible although it is not, but never the other way around, so only what both allow is possible.
    static BoolMask intersect(const BoolMask & left, const BoolMask & right)
    {
        return {
            left.can_be_true && right.can_be_true,
            left.can_be_false && right.can_be_false,
            left.can_be_null && right.can_be_null};
    }

    /// The following two special constants are used to speed up
    /// KeyCondition::checkInRange. When used as an initial_mask argument, they
    /// effectively prevent calculation of discarded BoolMask component as it is
    /// no longer changeable under BoolMask::combine (isComplete).
    static const BoolMask consider_only_can_be_true;
    static const BoolMask consider_only_can_be_false_or_null;
};

namespace fmt
{
template <>
struct formatter<BoolMask>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const BoolMask & mask, FormatContext & ctx)
    {
        return fmt::format_to(ctx.out(), "({}, {}, {})", mask.can_be_true, mask.can_be_false, mask.can_be_null);
    }
};
}
