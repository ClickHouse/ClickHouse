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
};
