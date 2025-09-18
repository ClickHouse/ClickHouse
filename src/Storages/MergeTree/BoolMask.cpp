#include "BoolMask.h"

/// BoolMask::can_be_X = true implies it will never change during BoolMask::combine.
const BoolMask BoolMask::consider_only_can_be_true(false, true);
const BoolMask BoolMask::consider_only_can_be_false(true, false);
