#pragma once

#include <Common/DynamicLoader/ElfTypes.h>

#include <cstddef>
#include <span>


namespace DB::DynamicLinker
{

/** Thread-local storage support for the general-dynamic TLS model.
  *
  * Background: a thread-local variable in a shared library is reached through a small helper the compiler
  * emits a call to - __tls_get_addr. It is given a "TLS index" (which module, and the offset inside that
  * module's per-thread block) and must return the address of that variable for the calling thread.
  *
  * The real dynamic linker keeps, per thread, a "dynamic thread vector" (DTV): an array mapping each module
  * to the address of that module's TLS block for this thread; blocks are allocated on first use from a
  * template copied out of the object file. We reimplement exactly this, independently of the host program's
  * own thread-local storage, so that libraries we load get their own thread-local variables.
  *
  * Limitation: this covers the general-dynamic and local-dynamic models (the default for -fPIC code). It does
  * NOT cover the initial-exec / local-exec models (R_*_TPOFF64), which address variables at a fixed offset
  * from the hardware thread pointer that the host's own C runtime owns and that we cannot extend. glibc's own
  * internal thread-local variables (such as errno) use initial-exec, which is one reason loading libc.so.6
  * itself needs extra cooperation beyond this loader.
  */

/// Register a module's thread-local template and receive a process-global module id (always >= 1).
/// `template_bytes` is the initialized portion (.tdata); `total_size` includes the zero-filled .tbss tail.
uint64_t registerThreadLocalModule(std::span<const std::byte> template_bytes, size_t total_size, size_t alignment);

/// Drop a module (on unload). Its id is never reused; blocks already allocated in live threads are freed lazily.
void unregisterThreadLocalModule(uint64_t module_id);

/// The address of the helper that loaded modules bind their "__tls_get_addr" references to.
/// (Kept as a typed function pointer so DynamicLoader can register it in its provided-symbols table.)
void * threadLocalStorageAccessor();

/// The helper itself: return the address, in the current thread, of the variable named by `index`.
void * getThreadLocalAddress(const ThreadLocalStorageIndex & index);

}
