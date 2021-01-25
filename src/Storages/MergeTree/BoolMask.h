#pragma once

/// Multiple Boolean values. That is, two Boolean values: can it be true, can it be false.
struct BoolMask
{
    bool can_be_true = false;
    bool can_be_false = false;

    BoolMask() {}
    BoolMask(bool can_be_true_, bool can_be_false_) : can_be_true(can_be_true_), can_be_false(can_be_false_) {}

    BoolMask operator &(const BoolMask & m)
    {
        return BoolMask(can_be_true && m.can_be_true, can_be_false || m.can_be_false);
    }
    BoolMask operator |(const BoolMask & m)
    {
        return BoolMask(can_be_true || m.can_be_true, can_be_false && m.can_be_false);
    }
    BoolMask operator !()
    {
        return BoolMask(can_be_false, can_be_true);
    }

    /// If mask is (true, true), then it can no longer change under operation |.
    /// We use this condition to early-exit KeyConditions::check{InRange,After} methods.
    bool isComplete() const
    {
        return can_be_false && can_be_true;
    }

    /// These special constants are used to implement KeyCondition::mayBeTrue{InRange,After} via KeyCondition::check{InRange,After}.
    /// When used as an initial_mask argument in KeyCondition::check{InRange,After} methods, they effectively prevent
    /// calculation of discarded BoolMask component as it is already set to true.
    static const BoolMask consider_only_can_be_true;
    static const BoolMask consider_only_can_be_false;
};
