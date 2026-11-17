#pragma once

#include <base/sanitizer_defs.h>
#include <base/types.h>

#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>


namespace DB
{

/// A hash of the type `F` that the JIT stores right before the entry of every compiled function of that type, like
/// Clang's KCFI does for native code.
template <typename F>
consteval UInt32 jitFunctionTag()
{
    UInt32 hash = 2166136261u; /// FNV-1a
    for (char c : std::string_view(__PRETTY_FUNCTION__))
        hash = (hash ^ static_cast<UInt8>(c)) * 16777619u;
    return hash;
}

/** Wrapper to call JIT-compiled functions.
  * UBSan's `-fsanitize=function` reads a type signature at `function_pointer - 8` before every indirect call.
  * JIT-compiled functions don't have this prologue, and when JIT code is at a page boundary,
  * the read accesses unmapped memory, causing a SIGSEGV.
  * For the same reason, Clang CFI (`cfi-icall`) cannot check the call: JIT code has no CFI type.
  * Instead, the call traps unless the tag before `f` is the one of its type.
  */
template <typename F, typename... Args>
NO_SANITIZE_UNDEFINED __attribute__((no_sanitize("cfi-icall"))) inline decltype(auto) callJITFunction(F && f, Args &&... args)
{
    UInt32 tag;
    memcpy(&tag, reinterpret_cast<const char *>(f) - sizeof(tag), sizeof(tag));
    if (tag != jitFunctionTag<std::decay_t<F>>())
        __builtin_trap();
    return f(std::forward<Args>(args)...);
}

}
