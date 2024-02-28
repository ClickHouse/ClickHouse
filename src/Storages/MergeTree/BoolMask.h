#pragma once

#include <fmt/format.h>

/// Multiple Boolean values. That is, two Boolean values: can it be true, can it be false.
struct BoolMask
{
    bool can_be_true;
    bool can_be_false;

    BoolMask(bool can_be_true_, bool can_be_false_) : can_be_true(can_be_true_), can_be_false(can_be_false_) { }

    BoolMask operator&(const BoolMask & m) const { return {can_be_true && m.can_be_true, can_be_false || m.can_be_false}; }
    BoolMask operator|(const BoolMask & m) const { return {can_be_true || m.can_be_true, can_be_false && m.can_be_false}; }
    BoolMask operator!() const { return {can_be_false, can_be_true}; }

    bool operator==(const BoolMask & other) const { return can_be_true == other.can_be_true && can_be_false == other.can_be_false; }

    /// Check if mask is no longer changeable under operation |.
    /// We use this condition to early-exit KeyConditions::check{InRange,After} methods.
    bool isComplete() const
    {
        if (*this == consider_only_can_be_true)
            return can_be_true;
        else if (*this == consider_only_can_be_false)
            return !can_be_false;
        return false;
    }

    /// These special constants are used to implement KeyCondition::mayBeTrue{InRange,After} via KeyCondition::check{InRange,After}.
    /// When used as an initial_mask argument in KeyCondition::check{InRange,After} methods, they effectively prevent
    /// calculation of discarded BoolMask component as it is already set to true.
    static const BoolMask consider_only_can_be_true;
    static const BoolMask consider_only_can_be_false;
    static const BoolMask consider_both;
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
        return fmt::format_to(ctx.out(), "({}, {})", mask.can_be_true, mask.can_be_false);
    }
};
}
