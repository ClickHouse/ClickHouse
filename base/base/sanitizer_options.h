#pragma once

/// Default sanitizer runtime options and LSan suppressions shared across ClickHouse executables.
///
/// The sanitizer runtime resolves the `__{asan,lsan,msan,tsan,ubsan}_default_*` hooks by symbol
/// name, so they must be defined with external "C" linkage exactly once per binary. Include this
/// header in a single translation unit of each executable (typically the one defining `main`).

#include <base/sanitizer_defs.h>

#ifdef SANITIZER
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
#pragma clang diagnostic ignored "-Wmissing-prototypes"
extern "C" {
#ifdef ADDRESS_SANITIZER
const char * __asan_default_options()
{
    return "halt_on_error=1 abort_on_error=1";
}
const char * __lsan_default_options()
{
    return "max_allocation_size_mb=32768";
}
const char * __lsan_default_suppressions()
{
    /// OpenSSL intentionally does not free all global state at exit.
    /// These are known false positives from the OpenSSL provider and EVP initialization.
    return "leak:ossl_provider_new\n"
           "leak:OSSL_PROVIDER_try_load_ex\n"
           "leak:ossl_rand_ctx_new\n"
           "leak:OSSL_LIB_CTX_new\n"
           "leak:ossl_legacy_provider_init\n"
           /// OpenSSL EVP method objects are cached in the provider method store and never freed at exit.
           /// Reached both via the AWS SDK (HMAC_Init_ex) and via direct EVP_MD_fetch in our functions (e.g. HMAC).
           "leak:evp_md_new\n"
           "leak:construct_evp_method\n"
           "leak:evp_generic_fetch\n"
           "leak:EVP_MD_fetch\n"
           "leak:ossl_method_construct\n"
           "leak:CRYPTO_THREAD_lock_new\n";
}
#endif

#ifdef MEMORY_SANITIZER
const char * __msan_default_options()
{
    return "abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768";
}
#endif

#ifdef THREAD_SANITIZER
const char * __tsan_default_options()
{
    return "halt_on_error=1 abort_on_error=1 history_size=7 second_deadlock_stack=1 max_allocation_size_mb=32768";
}
#endif

#ifdef UNDEFINED_BEHAVIOR_SANITIZER
const char * __ubsan_default_options()
{
    return "print_stacktrace=1 max_allocation_size_mb=32768";
}
#endif
}
#pragma clang diagnostic pop
#endif
