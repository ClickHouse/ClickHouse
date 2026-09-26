//
// winconfig.h
//
// Poco XML specific configuration for expat on Windows.
//
// The vendored expat sources include this file when `_WIN32` is defined; it is upstream
// expat's stand-in for the autotools-generated config. Everything expat configures through
// it - `BYTEORDER`, `HAVE_MEMMOVE`, `XML_CONTEXT_BYTES`, `<memory.h>`, `<string.h>` - is
// already set by `expat_config.h`, which the same sources include unconditionally just
// above. What is left is `<windows.h>`, which expat needs for `GetSystemTimeAsFileTime`
// when gathering entropy for its hash salt.
//
// Note that, unlike upstream's copy, this does not define and then `#undef`
// WIN32_LEAN_AND_MEAN: the ClickHouse build defines it for every translation unit (see
// cmake/target.cmake), and undefining it here would silently drop it for the rest of the
// file.
//

#ifndef WINCONFIG_H
#define WINCONFIG_H


#include <windows.h>


#endif /* ndef WINCONFIG_H */
