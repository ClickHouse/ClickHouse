#include <Storages/MergeTree/BoolMask.h>

/// BoolMask::can_be_X = true implies it will never change during BoolMask::combine.
constinit const BoolMask BoolMask::consider_only_can_be_true(false, true);
constinit const BoolMask BoolMask::consider_only_can_be_false(true, false);
