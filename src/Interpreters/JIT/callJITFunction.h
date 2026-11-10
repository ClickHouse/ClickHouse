#pragma once

#include <base/sanitizer_defs.h>

#include <typeinfo>
#include <utility>


namespace DB
{

/// The JIT-compiled functions that may be called through `callJITFunction`: the entries of the functions of live
/// modules, each with the type it was handed out as by `CHJIT::getFunction`.
void registerJITFunction(const void * function, const std::type_info & type);
bool isRegisteredJITFunction(const void * function, const std::type_info & type);

/** Wrapper to call JIT-compiled functions.
  * UBSan's `-fsanitize=function` reads a type signature at `function_pointer - 8` before every indirect call.
  * JIT-compiled functions don't have this prologue, and when JIT code is at a page boundary,
  * the read accesses unmapped memory, causing a SIGSEGV.
  * For the same reason, Clang CFI (`cfi-icall`) cannot check the call: JIT code has no CFI type.
  */
/// Instead of the CFI check, the call traps unless `f` is the entry of a live JIT-compiled function of type `F`.
template <typename F, typename... Args>
NO_SANITIZE_UNDEFINED __attribute__((no_sanitize("cfi-icall"))) inline decltype(auto) callJITFunction(F && f, Args &&... args)
{
    if (!isRegisteredJITFunction(reinterpret_cast<const void *>(f), typeid(std::decay_t<F>)))
        __builtin_trap();
    return f(std::forward<Args>(args)...);
}

}
