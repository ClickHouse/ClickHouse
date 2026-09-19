#pragma once

#include <Common/DynamicLoader/LoadedLibrary.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB::DynamicLinker
{

/** A self-contained dynamic loader: it can map a shared library (a ".so" built for the ordinary glibc
  * toolchain), wire up its relocations, load and link its dependencies recursively, and let you call
  * functions from it - all while living as a plain library inside a *statically linked* host binary that
  * has no dynamic linker of its own.
  *
  * It is deliberately independent of the host: a loaded library does not see, and must not use, any symbol
  * from the host program. Everything the library needs it must bring with it - if it calls malloc it must
  * pull in a C library of its own, which this loader will load and link just like any other dependency.
  *
  * Assumptions and limitations (see also ThreadLocalStorage.h):
  *   - The library must be well contained: memory it hands you must be released by its own functions, not
  *     by the host's free(), because the host and the loaded library have separate C runtimes.
  *   - Symbols are resolved within the opened library's own dependency closure, then against the table of
  *     symbols the host explicitly provides (see provideSymbol) - never against the host's own symbols.
  *   - Only the general-dynamic TLS model is supported. Initial-exec thread-local variables are rejected.
  *     This is why glibc's own libc.so.6 (which uses initial-exec TLS and imports the dynamic linker's
  *     private ABI) cannot be loaded as-is; a well-contained library with its own minimal runtime can.
  *
  * The public interface mirrors the familiar dlopen/dlsym/dlclose trio, spelled out in full.
  */
class DynamicLoader
{
public:
    struct Options
    {
        /// Make the host's memcpy/memmove/memset/memcmp available to loaded libraries. These are stateless and
        /// safe to share across the host/library runtime boundary, and are commonly needed even by tiny libraries.
        bool provide_host_memory_functions = true;

        /// Extra directories searched for dependencies, tried before the default system directories.
        std::vector<std::string> search_paths;
    };

    DynamicLoader() : DynamicLoader(Options{}) {}
    explicit DynamicLoader(Options options_);
    ~DynamicLoader();

    DynamicLoader(const DynamicLoader &) = delete;
    DynamicLoader & operator=(const DynamicLoader &) = delete;

    /// Register a symbol the host supplies to loaded libraries (e.g. a shim for a runtime the library expects).
    /// Used as the last resort when resolving imports, after the library's own dependency closure.
    void provideSymbol(const std::string & name, void * address);

    /// Add a directory to the front of the dependency search path.
    void addSearchPath(const std::string & directory);

    /// The counterpart of dlopen: map the library at `path`, load its dependencies, relocate, run its
    /// initializers, and return an opaque handle. Repeated opens of the same object share one instance.
    LoadedLibrary * open(const std::string & path);

    /// The counterpart of dlsym: find `name` in the library and its dependency closure. Returns nullptr if
    /// the symbol is not defined (an undefined-but-declared symbol is treated as absent).
    void * getSymbol(LoadedLibrary * library, const std::string & name);

    /// Convenience wrapper that casts the result of getSymbol to a function/object pointer type.
    template <typename Pointer>
    Pointer getSymbol(LoadedLibrary * library, const std::string & name)
    {
        return reinterpret_cast<Pointer>(getSymbol(library, name));
    }

    /// The counterpart of dlclose: drop one reference; when the last is gone, run finalizers and unmap.
    void close(LoadedLibrary * library);

private:
    Options options;

    /// The default system library directories for the host architecture (e.g. /usr/lib/aarch64-linux-gnu).
    std::vector<std::string> default_search_paths;
    std::vector<std::string> extra_search_paths;

    /// Symbols the host provides, consulted only after a library's own dependency closure.
    std::unordered_map<std::string, void *> provided_symbols;

    /// Loaded objects, deduplicated by resolved real path and by shared-object name (DT_SONAME).
    std::map<std::string, std::unique_ptr<LoadedLibrary>> libraries_by_real_path;
    std::unordered_map<std::string, LoadedLibrary *> libraries_by_shared_object_name;

    /// Resolve a needed library name to an absolute path using run paths, options, and system directories.
    std::string findLibrary(const std::string & name, const std::vector<std::string> & run_paths) const;

    /// Load one object (or return the already-loaded instance), mapping and parsing it but not yet relocating.
    LoadedLibrary * loadObject(const std::string & path, const std::string & requested_name);

    /// Breadth-first over DT_NEEDED starting at `root`, loading every dependency and filling dependency lists.
    void buildDependencyClosure(LoadedLibrary * root);

    /// Resolve `name`/`version` across `scope` (a breadth-first dependency list), then the provided symbols.
    ResolvedSymbol resolveSymbol(
        const std::vector<LoadedLibrary *> & scope, const char * name, const VersionRequirement & version) const;

    /// Run initializers depth-first so each object initializes after everything it depends on.
    void runInitializersInDependencyOrder(LoadedLibrary * root);
    void runInitializersRecursive(LoadedLibrary * library, std::vector<LoadedLibrary *> & already_initialized);

    /// Tear down `library` and any dependencies whose reference count reaches zero.
    void unloadRecursive(LoadedLibrary * library);
};

}
