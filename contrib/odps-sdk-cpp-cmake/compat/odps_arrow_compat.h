#pragma once

/// Arrow removed `arrow::decimal`. ODPS decimal precision is at most 38, so
/// the SDK's reachable calls all map to `arrow::decimal128`.
/// Parse Arrow itself before defining the source-compatibility macro so the
/// macro cannot rewrite any Arrow declaration.
#include <arrow/api.h>

#define decimal(precision, scale) decimal128(precision, scale)
