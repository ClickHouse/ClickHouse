# Dynamic loader

A self-contained userspace dynamic loader. It maps a shared library (an ordinary `.so` built for the glibc
toolchain), applies its relocations, loads and links its dependencies recursively, runs its initializers, and
lets you call functions from it — all while living as a plain library inside a **statically linked** host binary
that has no dynamic linker of its own. It is, in effect, a small independent `dlopen`/`dlsym`/`dlclose`.

## Why

A statically linked program cannot use the system `dlopen`, because there is no `ld.so` in the process to do the
work. This loader fills that gap: it is the part of `ld.so` that maps and relocates objects, reimplemented as a
library so a static binary can load plugins at run time.

## Design assumptions

The loaded library is treated as a closed world that shares nothing with the host:

- **It does not use any symbol from the host binary.** Symbols are resolved within the opened library's own
  dependency closure, and then against a table of symbols the host *explicitly* provides (see
  `DynamicLoader::provideSymbol`). The host's own symbol table is never searched.
- **It brings its own runtime.** If it calls `malloc`, it must pull in a C library of its own; the loader loads
  and links that library like any other dependency (`DT_NEEDED`).
- **It must be well contained.** Because the host and the loaded library have separate runtimes, memory the
  library hands you must be released by *its* functions, not by the host's `free`.

## Usage

```cpp
#include <Common/DynamicLoader/DynamicLoader.h>

using namespace DB::DynamicLinker;

DynamicLoader loader;
loader.addSearchPath("/path/to/plugins");

LoadedLibrary * library = loader.open("/path/to/plugins/libexample.so");
auto function = loader.getSymbol<int (*)(int, int)>(library, "example_add");
int result = function(2, 3);
loader.close(library);
```

To satisfy a symbol the library imports but that lives in the host (for example a shim for a runtime facility the
library expects), register it before opening:

```cpp
loader.provideSymbol("host_log", reinterpret_cast<void *>(&hostLog));
```

## What it does, step by step

For each object it loads, the loader:

1. **Maps the segments.** It reads the ELF header and *program headers*, reserves one contiguous region, and
   maps every `PT_LOAD` segment into it with the right permissions, zero-filling the `.bss` tail. The
   *load bias* — the difference between where the object was mapped and its link-time base address — is added to
   every link-time address afterwards.
2. **Parses the dynamic section.** It locates the string table, symbol table, symbol hash table (GNU or the
   classic System-V one), the relocation tables (`RELA`, PLT, and the compact `RELR` form), the symbol
   versioning tables, the thread-local template, the initializer/finalizer arrays, and the list of required
   libraries.
3. **Loads dependencies.** It follows `DT_NEEDED` breadth-first, searching run paths (`DT_RUNPATH`/`DT_RPATH`
   with `$ORIGIN` expanded), then explicit and `LD_LIBRARY_PATH` directories, then the default system
   directories, deduplicating shared dependencies.
4. **Applies relocations.** It rebases pointers (`RELATIVE`/`RELR`), fills the GOT/PLT (`GLOB_DAT`, `JUMP_SLOT`,
   absolute references), resolves indirect functions (`IRELATIVE`/`STT_GNU_IFUNC`), and wires up dynamic
   thread-local storage (`DTPMOD`/`DTPOFF`). Symbol lookups honor versioning.
5. **Hardens and initializes.** It makes the `PT_GNU_RELRO` region read-only, then runs `DT_INIT` and the
   `DT_INIT_ARRAY` functions with dependencies first. On `close`, finalizers run in reverse and the address
   space is unmapped once the last reference is gone.

The header `ElfTypes.h` contains a glossary that deciphers the terse Unix/ELF abbreviations (`phdr`, `vaddr`,
`filesz`, `DT_*`, `PLT`, `GOT`, `RELRO`, `DTV`, `TCB`, `IFUNC`, …) into full names.

## Supported platforms

x86-64 and AArch64 (little-endian, 64-bit, `ET_DYN` objects).

## Limitations

These are deliberate boundaries, reported as clear errors rather than silently mishandled:

- **glibc's own `libc.so.6` cannot be loaded as-is.** It uses initial-exec thread-local variables (such as
  `errno`) and imports the dynamic linker's private (`GLIBC_PRIVATE`) interface. Both are outside what a loader
  inside a static host can provide. A well-contained library that carries its own minimal runtime works; loading
  the system glibc would additionally require shimming that private interface via `provideSymbol`.
- **Only the general-dynamic thread-local storage model is supported** (accesses routed through
  `__tls_get_addr`, which the loader supplies from its own per-thread implementation). Initial-exec and
  local-exec variables (`R_*_TPOFF64`) address storage at a fixed offset from the hardware thread pointer that
  the host runtime owns and cannot be extended. The TLS-descriptor model (`R_*_TLSDESC`) is likewise not
  supported. Note that GCC on AArch64 defaults to the descriptor dialect, so libraries with thread-local storage
  should be built with `-mtls-dialect=trad` (x86-64 already defaults to the general-dynamic model).
- **`REL` relocations** (without an explicit addend) and **text relocations** (`DT_TEXTREL`, which write into
  read-only code) are not supported; 64-bit objects normally use `RELA` and position-independent code.
