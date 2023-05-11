#pragma once
#include <Common/FoundationDB/fdb_c_fwd.h>

// Copy from contrib/foundationdb/flow/error_definitions.h


namespace DB
{
// clang-format off
struct FDBErrorCode
{
#define ERROR(name, code, desc) static constexpr fdb_error_t name = (code);
#include <foundationdb/error_definitions.h>
#undef ERROR
};
// clang-format on
}
