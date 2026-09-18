/// ClickHouse builds `clang_rt_xray` without the upstream logging modes
/// (`xray-basic`, `xray-fdr`, `xray-profiling`): the binary is compiled with
/// `-fxray-modes=none` and the sleds are driven through `SYSTEM INSTRUMENT`.
/// The runtime still parses the `XRAY_OPTIONS` flags that select those modes,
/// so without this check a request such as
/// `XRAY_OPTIONS="patch_premain=true xray_mode=xray-fdr"` would patch every
/// sled before `main` and then run with a null handler: all of the overhead
/// and none of the output. Fail fast instead.
///
/// Upstream registers each mode from a static initializer that reads the flags
/// directly. That relies on `__xray_init` having already run from
/// `.preinit_array`, which musl does not process, so on musl those initializers
/// would read a zero-initialized `xray_mode` and crash. Here `__xray_init` is
/// called explicitly: it is idempotent and always leaves the flags parsed, even
/// when the executable has no instrumentation map.

#include "xray/xray_defs.h"
#include "xray/xray_flags.h"
#include "sanitizer_common/sanitizer_common.h"
#include "sanitizer_common/sanitizer_libc.h"

#include <xray/xray_interface.h>

namespace
{

constexpr const char * not_bundled
    = "but this build of the XRay runtime bundles no logging mode (xray-basic, xray-fdr, xray-profiling). Use SYSTEM INSTRUMENT instead.\n";

__attribute__((constructor)) void rejectUnavailableXRayModes() XRAY_NEVER_INSTRUMENT
{
    __xray_init();

    const auto * f = __xray::flags();

    if (f->xray_naive_log)
        __sanitizer::Report("XRay: XRAY_OPTIONS requests xray_naive_log=true, %s", not_bundled);
    else if (f->xray_fdr_log)
        __sanitizer::Report("XRay: XRAY_OPTIONS requests xray_fdr_log=true, %s", not_bundled);
    else if (f->xray_mode != nullptr && __sanitizer::internal_strlen(f->xray_mode) != 0)
        __sanitizer::Report("XRay: XRAY_OPTIONS requests xray_mode=%s, %s", f->xray_mode, not_bundled);
    else
        return;

    __sanitizer::Die();
}

}
