#pragma once

/// Множество значений булевой переменной. То есть два булевых значения: может ли быть true, может ли быть false.
struct BoolMask
{
	bool can_be_true;
	bool can_be_false;

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
