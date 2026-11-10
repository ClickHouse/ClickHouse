#pragma once

#include <base/sanitizer_defs.h>

#include <utility>


namespace DB
{

/** Wrapper to call JIT-compiled functions.
  * UBSan's `-fsanitize=function` reads a type signature at `function_pointer - 8` before every indirect call.
  * JIT-compiled functions don't have this prologue, and when JIT code is at a page boundary,
  * the read accesses unmapped memory, causing a SIGSEGV.
  * For the same reason, Clang CFI (`cfi-icall`) cannot check the call: JIT code has no CFI type.
  */
template <typename F, typename... Args>
NO_SANITIZE_UNDEFINED __attribute__((no_sanitize("cfi-icall"))) inline decltype(auto) callJITFunction(F && f, Args &&... args)
{
    return f(std::forward<Args>(args)...);
}

}
