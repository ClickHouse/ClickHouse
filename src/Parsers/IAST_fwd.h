#pragma once

#include <memory>
#include <absl/container/inlined_vector.h>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;
/// sizeof(absl::InlinedVector<ASTPtr, N>) == 8 + N * 16.
/// 7 elements take 120 Bytes which is ~128
using ASTs = absl::InlinedVector<ASTPtr, 7>;

}
