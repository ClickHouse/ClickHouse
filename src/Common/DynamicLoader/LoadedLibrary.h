#pragma once

#include <Common/DynamicLoader/ElfTypes.h>

#include <cstddef>
#include <functional>
#include <span>
#include <string>
#include <vector>


namespace DB::DynamicLinker
{

/// A version constraint attached to an imported symbol, e.g. it wants "memcpy" specifically at version "GLIBC_2.14".
struct VersionRequirement
{
    const char * name = nullptr;    /// nullptr means "any version" (an unversioned reference).
    bool weak = false;              /// A weak requirement may go unsatisfied without an error.
};

class LoadedLibrary;

/// The outcome of resolving a symbol name across a set of libraries.
struct ResolvedSymbol
{
    const LoadedLibrary * library = nullptr;    /// The library that defines the symbol (null for a host-provided one).
    const Symbol * symbol = nullptr;            /// The symbol table entry in that library (null for a host-provided one).
    void * address = nullptr;                   /// Final run-time address (IFUNC already resolved for code symbols).
    bool found = false;                         /// Whether a definition was found at all (address may legitimately be null).

    explicit operator bool() const { return found; }
};

/// Called by relocation code to find where an imported symbol lives, searching the whole load scope.
using SymbolResolver = std::function<ResolvedSymbol(const char * name, const VersionRequirement & version)>;

/** One shared object mapped into memory and prepared for use.
  *
  * The lifecycle, driven by DynamicLoader, is:
  *   1. map()                - reserve address space and map the PT_LOAD segments.
  *   2. parseDynamicSection() - locate the string/symbol/relocation/hash/version/TLS tables.
  *   3. (DynamicLoader loads dependencies named by neededLibraries())
  *   4. applyRelocations()   - patch the GOT/data using a resolver over the whole scope.
  *   5. protectReadOnlyRelocations() - re-protect the RELRO region.
  *   6. runInitializers()    - call DT_INIT and the DT_INIT_ARRAY functions.
  * and on teardown:
  *   7. runFinalizers()      - call DT_FINI_ARRAY and DT_FINI.
  *   8. unmap()              - release the address space.
  */
class LoadedLibrary
{
public:
    LoadedLibrary(std::string path_, std::string requested_name_);
    ~LoadedLibrary();

    LoadedLibrary(const LoadedLibrary &) = delete;
    LoadedLibrary & operator=(const LoadedLibrary &) = delete;

    /// Steps 1-2. Reads the file, maps segments, and parses the dynamic section.
    void mapAndParse();

    /// The library names this object requires (from DT_NEEDED), to be loaded by DynamicLoader.
    const std::vector<std::string> & neededLibraries() const { return needed_libraries; }

    /// Directories to prepend to the dependency search path (from DT_RUNPATH / DT_RPATH), with $ORIGIN expanded.
    const std::vector<std::string> & runPaths() const { return run_paths; }

    /// The canonical name this object advertises (DT_SONAME), or its requested name if it has none.
    const std::string & sharedObjectName() const { return shared_object_name; }
    const std::string & path() const { return file_path; }

    /// Step 4. `resolver` maps an imported name to its definition anywhere in the load scope.
    void applyRelocations(const SymbolResolver & resolver);

    /// Step 5. Make the RELRO ("relocations read-only") region read-only to harden the GOT.
    void protectReadOnlyRelocations();

    /// Steps 6/7. Run/undo static initializers and finalizers. Idempotent.
    void runInitializers();
    void runFinalizers();

    /// Look up a symbol defined by this object only (used both by dlsym and by the scope-wide resolver).
    ResolvedSymbol lookup(const char * name, const VersionRequirement & version) const;

    /// Turn a symbol into its run-time address, calling the IFUNC resolver for indirect functions.
    void * addressOfSymbol(const Symbol & symbol) const;

    /// The version name an imported symbol requires (for the resolver to match against), or nullptr.
    VersionRequirement importedSymbolVersion(uint32_t symbol_index) const;

    /// TLS accounting. A library with a PT_TLS segment registers itself and gets a module id in mapAndParse().
    bool hasThreadLocalStorage() const { return tls_memory_size != 0; }
    uint64_t threadLocalStorageModuleID() const { return tls_module_id; }

    /// Reference counting so a shared dependency is torn down only once its last user closes it.
    void incrementReferenceCount() { ++reference_count; }
    size_t decrementReferenceCount() { return --reference_count; }
    size_t referenceCount() const { return reference_count; }

    /// The object's direct dependencies (one per DT_NEEDED). Filled in by DynamicLoader.
    std::vector<LoadedLibrary *> & dependencyList() { return dependency_list; }
    const std::vector<LoadedLibrary *> & dependencyList() const { return dependency_list; }

private:
    std::string file_path;              /// Absolute path we loaded from.
    std::string requested_name;         /// The name the caller / DT_NEEDED asked for.
    std::string shared_object_name;     /// DT_SONAME (falls back to requested_name).

    /// Mapping.
    std::byte * load_address = nullptr; /// Start of the reserved region we mapped the object into.
    size_t mapped_size = 0;             /// Size of that reserved region.
    uint64_t load_bias = 0;             /// Added to every link-time address to get a run-time address.

    /// The RELRO region (page-aligned), if any.
    std::byte * relro_start = nullptr;
    size_t relro_size = 0;

    /// Dynamic linking tables (all pointers already adjusted by load_bias).
    const char * string_table = nullptr;
    size_t string_table_size = 0;
    const Symbol * symbol_table = nullptr;

    const RelocationWithAddend * rela_relocations = nullptr;
    size_t rela_relocations_count = 0;
    const RelocationWithAddend * plt_relocations = nullptr;
    size_t plt_relocations_count = 0;
    const uint64_t * relr_relocations = nullptr;
    size_t relr_relocations_count = 0;

    /// Symbol hash tables (we use whichever is present to look symbols up quickly).
    const uint32_t * gnu_hash_table = nullptr;
    const uint32_t * sysv_hash_table = nullptr;
    size_t symbol_count = 0;            /// Total symbols (derived from the hash table).

    /// Symbol versioning tables.
    const uint16_t * version_symbols = nullptr;         /// DT_VERSYM: one version index per symbol.
    std::vector<std::string> version_names_by_index;    /// version index -> version name.

    /// Initializers / finalizers.
    void (*init_function)() = nullptr;
    void (*fini_function)() = nullptr;
    std::span<void (* const)()> init_array;
    std::span<void (* const)()> fini_array;

    /// Thread-local storage template (the bytes copied into each thread's TLS block).
    std::span<const std::byte> tls_template;
    size_t tls_memory_size = 0;
    size_t tls_alignment = 1;
    uint64_t tls_module_id = 0;

    std::vector<std::string> needed_libraries;
    std::vector<std::string> run_paths;
    std::vector<LoadedLibrary *> dependency_list;

    bool text_relocations = false;      /// DT_TEXTREL: relocations write into read-only code.
    bool dependencies_built = false;    /// DynamicLoader has already followed this object's DT_NEEDED.
    bool relocated = false;             /// DynamicLoader has already applied this object's relocations.
    bool initializers_ran = false;
    bool finalizers_ran = false;
    size_t reference_count = 0;

    /// Helpers used by mapAndParse().
    void mapSegments(int file_descriptor, const ProgramHeader * program_headers, size_t program_header_count);
    void parseDynamicSection(const DynamicEntry * dynamic_section);
    void buildVersionIndex(
        const void * version_definitions, uint16_t definitions_count,
        const void * version_needs, uint16_t needs_count);
    size_t countSymbols() const;

    /// Helpers used by lookup(). Walks the appropriate hash chain, honoring symbol versioning.
    const Symbol * findDefinedSymbol(const char * name, const VersionRequirement & version) const;
    const char * symbolName(const Symbol & symbol) const { return string_table + symbol.name_offset; }
    const char * versionNameOfSymbol(uint32_t symbol_index) const;

    /// Relocation helpers.
    ResolvedSymbol resolveForRelocation(uint32_t symbol_index, const SymbolResolver & resolver) const;
    void applyOneRelocation(const RelocationWithAddend & relocation, const SymbolResolver & resolver);
    void applyCompactRelativeRelocations();

    friend class DynamicLoader;
};

}
