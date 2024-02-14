#pragma once
#include <Common/FoundationDB/fdb_c_fwd.h>

// Copy from contrib/foundationdb/flow/error_definitions.h


namespace DB
{
// clang-format off
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wextra-semi"
struct FDBErrorCode
{
#define ERROR(name, code, desc) static constexpr fdb_error_t name = (code);
#include <foundationdb/error_definitions.h>
#undef ERROR
};
#pragma clang diagnostic pop
// clang-format on
}
