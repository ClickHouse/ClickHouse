#include <Common/DynamicLoader/LoadedLibrary.h>
#include <Common/DynamicLoader/ThreadLocalStorage.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cstring>

#include <fcntl.h>
#include <sys/auxv.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

extern "C" char ** environ;


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ELF;
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MPROTECT;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSTAT;
    extern const int NOT_IMPLEMENTED;
}

namespace DynamicLinker
{

namespace
{
    size_t pageSize()
    {
        static const size_t page_size = static_cast<size_t>(::sysconf(_SC_PAGESIZE));
        return page_size;
    }

    uint64_t roundDownToPage(uint64_t value)
    {
        return value & ~(pageSize() - 1);
    }

    uint64_t roundUpToPage(uint64_t value)
    {
        return roundDownToPage(value + pageSize() - 1);
    }

    /// Translate ELF segment permission flags into the mmap/mprotect protection bits.
    int segmentProtection(uint32_t segment_flags)
    {
        int protection = 0;
        if (segment_flags & SEGMENT_READABLE)
            protection |= PROT_READ;
        if (segment_flags & SEGMENT_WRITABLE)
            protection |= PROT_WRITE;
        if (segment_flags & SEGMENT_EXECUTABLE)
            protection |= PROT_EXEC;
        return protection;
    }

    /// The GNU hash of a symbol name (the "djb2" hash: h = h * 33 + c).
    uint32_t computeGNUHash(const char * name)
    {
        uint32_t hash = 5381;
        for (const auto * pointer = reinterpret_cast<const unsigned char *>(name); *pointer; ++pointer)
            hash = hash * 33 + *pointer;
        return hash;
    }

    /// The classic System-V ELF hash of a symbol name.
    uint32_t computeSystemVHash(const char * name)
    {
        uint32_t hash = 0;
        for (const auto * pointer = reinterpret_cast<const unsigned char *>(name); *pointer; ++pointer)
        {
            hash = (hash << 4) + *pointer;
            uint32_t high_bits = hash & 0xf0000000u;
            if (high_bits)
                hash ^= high_bits >> 24;
            hash &= ~high_bits;
        }
        return hash;
    }

    /// Call the resolver of an indirect function (IFUNC) and return the real address it selects.
    /// The resolver is passed the hardware capability bitmask; extra arguments a simpler resolver ignores.
    void * callIndirectFunctionResolver(uint64_t resolver_address)
    {
        using ResolverFunction = void * (*)(uint64_t hardware_capabilities, void * unused);
        auto resolver = reinterpret_cast<ResolverFunction>(resolver_address);
        return resolver(::getauxval(AT_HWCAP), nullptr);
    }
}


LoadedLibrary::LoadedLibrary(std::string path_, std::string requested_name_)
    : file_path(std::move(path_)), requested_name(std::move(requested_name_))
{
}

LoadedLibrary::~LoadedLibrary()
{
    /// Best effort teardown: run finalizers, drop the TLS module, and return the address space.
    try
    {
        runFinalizers();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    if (tls_module_id != 0)
        unregisterThreadLocalModule(tls_module_id);

    if (load_address != nullptr)
        ::munmap(load_address, mapped_size);
}


void LoadedLibrary::mapAndParse()
{
    int file_descriptor = ::open(file_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (file_descriptor < 0)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open shared library '{}'", file_path);

    /// Everything below must close the descriptor; use a tiny scope guard.
    struct DescriptorCloser
    {
        int descriptor;
        ~DescriptorCloser() { ::close(descriptor); }
    } descriptor_closer{file_descriptor};

    struct stat file_status;
    if (::fstat(file_descriptor, &file_status) != 0)
        throw Exception(ErrorCodes::CANNOT_FSTAT, "Cannot fstat shared library '{}'", file_path);

    /// Read and validate the ELF header.
    ElfHeader header;
    if (::pread(file_descriptor, &header, sizeof(header), 0) != static_cast<ssize_t>(sizeof(header)))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Cannot read ELF header of '{}'", file_path);

    if (std::memcmp(header.identification, "\x7F""ELF", 4) != 0)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "File '{}' is not an ELF object", file_path);
    if (header.identification[ELF_CLASS_INDEX] != ELFCLASS64 || header.identification[ELF_DATA_INDEX] != ELFDATA2LSB)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Only little-endian 64-bit ELF is supported ('{}')", file_path);
    if (header.type != ET_DYN)
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "'{}' is not a shared object (ELF type is not ET_DYN)", file_path);
    if (header.machine != HOST_ELF_MACHINE)
        throw Exception(ErrorCodes::CANNOT_DLOPEN,
            "'{}' was built for a different architecture (ELF machine {}, host expects {})",
            file_path, header.machine, HOST_ELF_MACHINE);

    /// Read the program header table.
    if (header.program_header_entry_size != sizeof(ProgramHeader))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Unexpected program header entry size in '{}'", file_path);

    std::vector<ProgramHeader> program_headers(header.program_header_count);
    size_t program_headers_bytes = program_headers.size() * sizeof(ProgramHeader);
    if (::pread(file_descriptor, program_headers.data(), program_headers_bytes, header.program_header_offset)
        != static_cast<ssize_t>(program_headers_bytes))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Cannot read program headers of '{}'", file_path);

    /// Map the loadable segments into memory.
    mapSegments(file_descriptor, program_headers.data(), program_headers.size());

    /// Locate the dynamic section, the thread-local template, and the read-only-relocations region.
    const DynamicEntry * dynamic_section = nullptr;
    for (const auto & segment : program_headers)
    {
        switch (segment.type)
        {
            case static_cast<uint32_t>(SegmentType::Dynamic):
                dynamic_section = reinterpret_cast<const DynamicEntry *>(load_bias + segment.virtual_address);
                break;
            case static_cast<uint32_t>(SegmentType::ThreadLocalStorage):
                tls_template = std::span<const std::byte>(
                    reinterpret_cast<const std::byte *>(load_bias + segment.virtual_address), segment.file_size);
                tls_memory_size = segment.memory_size;
                tls_alignment = segment.alignment ? segment.alignment : 1;
                break;
            case static_cast<uint32_t>(SegmentType::GNURelocationsReadOnly):
                relro_start = reinterpret_cast<std::byte *>(roundDownToPage(load_bias + segment.virtual_address));
                relro_size = roundDownToPage(load_bias + segment.virtual_address + segment.memory_size)
                    - reinterpret_cast<uint64_t>(relro_start);
                break;
            default:
                break;
        }
    }

    if (dynamic_section == nullptr)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Shared object '{}' has no dynamic section", file_path);

    parseDynamicSection(dynamic_section);

    if (shared_object_name.empty())
        shared_object_name = requested_name;

    /// Register the thread-local template so our __tls_get_addr can serve this module's variables.
    if (hasThreadLocalStorage())
        tls_module_id = registerThreadLocalModule(tls_template, tls_memory_size, tls_alignment);
}


void LoadedLibrary::mapSegments(int file_descriptor, const ProgramHeader * program_headers, size_t program_header_count)
{
    /// Find the address span covered by all PT_LOAD segments.
    bool any_load = false;
    uint64_t lowest_address = 0;
    uint64_t highest_address = 0;
    for (size_t i = 0; i < program_header_count; ++i)
    {
        const ProgramHeader & segment = program_headers[i];
        if (segment.type != static_cast<uint32_t>(SegmentType::Load))
            continue;

        uint64_t segment_begin = roundDownToPage(segment.virtual_address);
        uint64_t segment_end = roundUpToPage(segment.virtual_address + segment.memory_size);
        if (!any_load)
        {
            lowest_address = segment_begin;
            highest_address = segment_end;
            any_load = true;
        }
        else
        {
            lowest_address = std::min(lowest_address, segment_begin);
            highest_address = std::max(highest_address, segment_end);
        }
    }

    if (!any_load)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Shared object '{}' has no loadable segments", file_path);

    /// Reserve one contiguous region for the whole object, so segment offsets stay consistent.
    mapped_size = highest_address - lowest_address;
    void * reservation = ::mmap(nullptr, mapped_size, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (reservation == MAP_FAILED)
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "Cannot reserve {} bytes of address space for '{}'", mapped_size, file_path);

    load_address = static_cast<std::byte *>(reservation);
    /// The load bias turns link-time (relative) addresses into run-time addresses.
    load_bias = reinterpret_cast<uint64_t>(load_address) - lowest_address;

    /// Map each loadable segment over the reservation.
    for (size_t i = 0; i < program_header_count; ++i)
    {
        const ProgramHeader & segment = program_headers[i];
        if (segment.type != static_cast<uint32_t>(SegmentType::Load))
            continue;

        int protection = segmentProtection(segment.flags);

        uint64_t segment_address = load_bias + segment.virtual_address;
        uint64_t mapping_address = roundDownToPage(segment_address);
        uint64_t page_offset = segment_address - mapping_address;
        uint64_t file_mapping_length = segment.file_size + page_offset;

        if (file_mapping_length != 0)
        {
            void * mapped = ::mmap(
                reinterpret_cast<void *>(mapping_address), file_mapping_length, protection,
                MAP_PRIVATE | MAP_FIXED, file_descriptor, segment.file_offset - page_offset);
            if (mapped == MAP_FAILED)
                throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                    "Cannot map a segment of '{}' at offset {}", file_path, segment.file_offset);
        }

        /// If the segment is larger in memory than in the file, the extra tail is zero-filled (".bss").
        if (segment.memory_size > segment.file_size)
        {
            uint64_t zero_begin = segment_address + segment.file_size;
            uint64_t zero_end = segment_address + segment.memory_size;

            /// Zero the remainder of the last file-backed page.
            uint64_t last_file_page_end = roundUpToPage(zero_begin);
            if (last_file_page_end > zero_begin)
            {
                uint64_t partial_end = std::min(last_file_page_end, zero_end);
                if (protection & PROT_WRITE)
                {
                    std::memset(reinterpret_cast<void *>(zero_begin), 0, partial_end - zero_begin);
                }
                else
                {
                    /// A read-only segment with a .bss tail is unusual; make it writable just for the memset.
                    uint64_t page_begin = roundDownToPage(zero_begin);
                    if (::mprotect(reinterpret_cast<void *>(page_begin), partial_end - page_begin, protection | PROT_WRITE) != 0)
                        throw Exception(ErrorCodes::CANNOT_MPROTECT, "Cannot zero the .bss tail of '{}'", file_path);
                    std::memset(reinterpret_cast<void *>(zero_begin), 0, partial_end - zero_begin);
                    ::mprotect(reinterpret_cast<void *>(page_begin), partial_end - page_begin, protection);
                }
            }

            /// Map fresh anonymous (already-zeroed) pages for the rest of the .bss.
            if (zero_end > last_file_page_end)
            {
                void * mapped = ::mmap(
                    reinterpret_cast<void *>(last_file_page_end), zero_end - last_file_page_end, protection,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
                if (mapped == MAP_FAILED)
                    throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot map the .bss of '{}'", file_path);
            }
        }
    }
}


void LoadedLibrary::parseDynamicSection(const DynamicEntry * dynamic_section)
{
    /// Addresses that need collecting before the string table is known.
    std::vector<uint64_t> needed_name_offsets;
    std::vector<uint64_t> run_path_offsets;
    uint64_t shared_object_name_offset = 0;
    bool has_shared_object_name = false;

    const void * version_definitions = nullptr;
    const void * version_needs = nullptr;
    uint16_t version_definitions_count = 0;
    uint16_t version_needs_count = 0;

    uint64_t init_array_size = 0;
    uint64_t fini_array_size = 0;
    void (**init_array_data)() = nullptr;
    void (**fini_array_data)() = nullptr;

    uint64_t plt_relocations_bytes = 0;

    for (const DynamicEntry * entry = dynamic_section; entry->tag != static_cast<int64_t>(DynamicTag::Null); ++entry)
    {
        /// Table addresses are link-time addresses and need the load bias; sizes and offsets do not.
        uint64_t biased = load_bias + entry->value;
        switch (static_cast<DynamicTag>(entry->tag))
        {
            case DynamicTag::StringTable:       string_table = reinterpret_cast<const char *>(biased); break;
            case DynamicTag::StringTableSize:   string_table_size = entry->value; break;
            case DynamicTag::SymbolTable:       symbol_table = reinterpret_cast<const Symbol *>(biased); break;
            case DynamicTag::Hash:              sysv_hash_table = reinterpret_cast<const uint32_t *>(biased); break;
            case DynamicTag::GNUHash:           gnu_hash_table = reinterpret_cast<const uint32_t *>(biased); break;

            case DynamicTag::RelocationsWithAddend:     rela_relocations = reinterpret_cast<const RelocationWithAddend *>(biased); break;
            case DynamicTag::RelocationsWithAddendSize: rela_relocations_count = entry->value / sizeof(RelocationWithAddend); break;
            case DynamicTag::PLTRelocations:            plt_relocations = reinterpret_cast<const RelocationWithAddend *>(biased); break;
            case DynamicTag::PLTRelocationsSize:        plt_relocations_bytes = entry->value; break;
            case DynamicTag::PLTRelocationType:
                if (entry->value != DT_RELA)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Shared object '{}' uses REL (not RELA) PLT relocations, which are not supported", file_path);
                break;
            case DynamicTag::RelativeRelocations:       relr_relocations = reinterpret_cast<const uint64_t *>(biased); break;
            case DynamicTag::RelativeRelocationsSize:   relr_relocations_count = entry->value / sizeof(uint64_t); break;

            case DynamicTag::Relocations:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Shared object '{}' uses REL (not RELA) relocations, which are not supported", file_path);

            case DynamicTag::Init:      init_function = reinterpret_cast<void (*)()>(biased); break;
            case DynamicTag::Fini:      fini_function = reinterpret_cast<void (*)()>(biased); break;
            case DynamicTag::InitArray: init_array_data = reinterpret_cast<void (**)()>(biased); break;
            case DynamicTag::FiniArray: fini_array_data = reinterpret_cast<void (**)()>(biased); break;
            case DynamicTag::InitArraySize: init_array_size = entry->value; break;
            case DynamicTag::FiniArraySize: fini_array_size = entry->value; break;

            case DynamicTag::Needed:            needed_name_offsets.push_back(entry->value); break;
            case DynamicTag::RunPath:           run_path_offsets.push_back(entry->value); break;
            case DynamicTag::RunPathDeprecated: run_path_offsets.push_back(entry->value); break;
            case DynamicTag::SharedObjectName:  shared_object_name_offset = entry->value; has_shared_object_name = true; break;

            case DynamicTag::VersionSymbol:          version_symbols = reinterpret_cast<const uint16_t *>(biased); break;
            case DynamicTag::VersionDefinitions:     version_definitions = reinterpret_cast<const void *>(biased); break;
            case DynamicTag::VersionDefinitionsCount: version_definitions_count = static_cast<uint16_t>(entry->value); break;
            case DynamicTag::VersionNeeded:          version_needs = reinterpret_cast<const void *>(biased); break;
            case DynamicTag::VersionNeededCount:     version_needs_count = static_cast<uint16_t>(entry->value); break;

            case DynamicTag::TextRelocations:
                text_relocations = true;
                break;

            default:
                break;
        }
    }

    if (string_table == nullptr || symbol_table == nullptr)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF,
            "Shared object '{}' has no string table or symbol table", file_path);

    if (text_relocations)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Shared object '{}' needs writable-text relocations (DT_TEXTREL), which are not supported", file_path);

    plt_relocations_count = plt_relocations_bytes / sizeof(RelocationWithAddend);
    if (init_array_data)
        init_array = std::span<void (* const)()>(init_array_data, init_array_size / sizeof(void (*)()));
    if (fini_array_data)
        fini_array = std::span<void (* const)()>(fini_array_data, fini_array_size / sizeof(void (*)()));

    symbol_count = countSymbols();

    /// Resolve the collected string-table offsets now that we know where the string table is.
    auto stringAt = [this](uint64_t offset) -> std::string
    {
        if (offset >= string_table_size)
            throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "String offset out of bounds in '{}'", file_path);
        return std::string(string_table + offset);
    };

    for (uint64_t offset : needed_name_offsets)
        needed_libraries.push_back(stringAt(offset));
    for (uint64_t offset : run_path_offsets)
        run_paths.push_back(stringAt(offset));
    if (has_shared_object_name)
        shared_object_name = stringAt(shared_object_name_offset);

    buildVersionIndex(version_definitions, version_definitions_count, version_needs, version_needs_count);
}


size_t LoadedLibrary::countSymbols() const
{
    /// The System-V hash table records the symbol count directly (its second word is "nchain").
    if (sysv_hash_table != nullptr)
        return sysv_hash_table[1];

    /// The GNU hash table does not; the highest symbol index is found by walking to the end of the longest chain.
    if (gnu_hash_table != nullptr)
    {
        uint32_t bucket_count = gnu_hash_table[0];
        uint32_t symbol_offset = gnu_hash_table[1];
        uint32_t bloom_word_count = gnu_hash_table[2];
        const uint32_t * buckets = gnu_hash_table + 4 + bloom_word_count * (sizeof(uint64_t) / sizeof(uint32_t));
        const uint32_t * chain = buckets + bucket_count;

        uint32_t highest = 0;
        for (uint32_t i = 0; i < bucket_count; ++i)
            highest = std::max(highest, buckets[i]);
        if (highest < symbol_offset)
            return symbol_offset;
        while (!(chain[highest - symbol_offset] & 1))
            ++highest;
        return highest + 1;
    }

    return 0;
}


void LoadedLibrary::buildVersionIndex(
    const void * version_definitions, uint16_t definitions_count,
    const void * version_needs, uint16_t needs_count)
{
    if (version_symbols == nullptr)
        return;

    /// Both the definitions this object provides and the versions it requires are numbered in one index space;
    /// build a single "version index -> version name" table covering both.
    auto ensureSize = [this](size_t index)
    {
        if (index >= version_names_by_index.size())
            version_names_by_index.resize(index + 1);
    };

    if (version_definitions != nullptr)
    {
        const auto * definition = static_cast<const VersionDefinition *>(version_definitions);
        for (uint16_t i = 0; i < definitions_count; ++i)
        {
            /// The first auxiliary entry holds the version's own name.
            const auto * aux = reinterpret_cast<const VersionDefinitionAux *>(
                reinterpret_cast<const char *>(definition) + definition->vd_aux);
            uint16_t index = definition->vd_ndx & VERSION_INDEX_MASK;
            ensureSize(index);
            version_names_by_index[index] = string_table + aux->vda_name;

            if (definition->vd_next == 0)
                break;
            definition = reinterpret_cast<const VersionDefinition *>(
                reinterpret_cast<const char *>(definition) + definition->vd_next);
        }
    }

    if (version_needs != nullptr)
    {
        const auto * need = static_cast<const VersionNeed *>(version_needs);
        for (uint16_t i = 0; i < needs_count; ++i)
        {
            const auto * aux = reinterpret_cast<const VersionNeedAux *>(
                reinterpret_cast<const char *>(need) + need->vn_aux);
            for (;;)
            {
                uint16_t index = aux->vna_other & VERSION_INDEX_MASK;
                ensureSize(index);
                version_names_by_index[index] = string_table + aux->vna_name;
                if (aux->vna_next == 0)
                    break;
                aux = reinterpret_cast<const VersionNeedAux *>(reinterpret_cast<const char *>(aux) + aux->vna_next);
            }

            if (need->vn_next == 0)
                break;
            need = reinterpret_cast<const VersionNeed *>(reinterpret_cast<const char *>(need) + need->vn_next);
        }
    }
}


const char * LoadedLibrary::versionNameOfSymbol(uint32_t symbol_index) const
{
    if (version_symbols == nullptr)
        return nullptr;
    uint16_t index = version_symbols[symbol_index] & VERSION_INDEX_MASK;
    /// Indices 0 (local) and 1 (global base) mean "no explicit version".
    if (index <= VERSION_GLOBAL || index >= version_names_by_index.size())
        return nullptr;
    const std::string & name = version_names_by_index[index];
    return name.empty() ? nullptr : name.c_str();
}


const Symbol * LoadedLibrary::findDefinedSymbol(const char * name, const VersionRequirement & version) const
{
    const Symbol * best_default = nullptr;

    /// A single lambda checks one candidate index and updates the running best match.
    /// Returns true when an exact version match is found and the search can stop.
    const Symbol * exact_match = nullptr;
    auto consider = [&](uint32_t index) -> bool
    {
        const Symbol & symbol = symbol_table[index];
        if (!symbol.isDefined() || symbol.binding() == STB_LOCAL)
            return false;
        if (std::strcmp(name, symbolName(symbol)) != 0)
            return false;

        bool is_hidden = version_symbols != nullptr && (version_symbols[index] & VERSION_HIDDEN_FLAG);
        const char * candidate_version = versionNameOfSymbol(index);

        if (version.name != nullptr)
        {
            if (candidate_version != nullptr && std::strcmp(candidate_version, version.name) == 0)
            {
                exact_match = &symbol;
                return true;
            }
            /// A default (non-hidden) definition is an acceptable fallback if no exact version matches.
            if (!is_hidden && best_default == nullptr)
                best_default = &symbol;
        }
        else
        {
            /// An unversioned reference prefers the default definition.
            if (!is_hidden)
            {
                exact_match = &symbol;
                return true;
            }
            if (best_default == nullptr)
                best_default = &symbol;
        }
        return false;
    };

    uint32_t name_hash;
    if (gnu_hash_table != nullptr)
    {
        uint32_t bucket_count = gnu_hash_table[0];
        uint32_t symbol_offset = gnu_hash_table[1];
        uint32_t bloom_word_count = gnu_hash_table[2];
        uint32_t bloom_shift = gnu_hash_table[3];
        const auto * bloom = reinterpret_cast<const uint64_t *>(gnu_hash_table + 4);
        const uint32_t * buckets = reinterpret_cast<const uint32_t *>(bloom + bloom_word_count);
        const uint32_t * chain = buckets + bucket_count;

        name_hash = computeGNUHash(name);

        /// The Bloom filter cheaply rules out names that are certainly absent.
        uint64_t bloom_word = bloom[(name_hash / 64) % bloom_word_count];
        uint64_t bloom_mask = (1ull << (name_hash % 64)) | (1ull << ((name_hash >> bloom_shift) % 64));
        if ((bloom_word & bloom_mask) != bloom_mask)
            return nullptr;

        uint32_t index = buckets[name_hash % bucket_count];
        if (index < symbol_offset)
            return nullptr;
        for (;;)
        {
            uint32_t chain_hash = chain[index - symbol_offset];
            /// The chain stores hashes with the low bit reserved as an end-of-chain flag.
            if (((chain_hash ^ name_hash) >> 1) == 0 && consider(index))
                return exact_match;
            if (chain_hash & 1)
                break;
            ++index;
        }
    }
    else if (sysv_hash_table != nullptr)
    {
        uint32_t bucket_count = sysv_hash_table[0];
        const uint32_t * buckets = sysv_hash_table + 2;
        const uint32_t * chain = buckets + bucket_count;
        name_hash = computeSystemVHash(name);
        for (uint32_t index = buckets[name_hash % bucket_count]; index != STN_UNDEF; index = chain[index])
            if (consider(index))
                return exact_match;
    }

    return exact_match ? exact_match : best_default;
}


ResolvedSymbol LoadedLibrary::lookup(const char * name, const VersionRequirement & version) const
{
    const Symbol * symbol = findDefinedSymbol(name, version);
    if (symbol == nullptr)
        return {};
    return ResolvedSymbol{this, symbol, addressOfSymbol(*symbol), true};
}


void * LoadedLibrary::addressOfSymbol(const Symbol & symbol) const
{
    uint64_t address = load_bias + symbol.value;
    if (symbol.isIndirectFunction() && symbol.isDefined())
        return callIndirectFunctionResolver(address);
    return reinterpret_cast<void *>(address);
}


VersionRequirement LoadedLibrary::importedSymbolVersion(uint32_t symbol_index) const
{
    VersionRequirement requirement;
    requirement.name = versionNameOfSymbol(symbol_index);
    requirement.weak = symbol_table[symbol_index].isWeak();
    return requirement;
}


ResolvedSymbol LoadedLibrary::resolveForRelocation(uint32_t symbol_index, const SymbolResolver & resolver) const
{
    const Symbol & symbol = symbol_table[symbol_index];
    const char * name = symbolName(symbol);

    /// A local definition is bound directly; it is not visible to (and need not be searched for in) the scope.
    if (symbol.isDefined() && symbol.binding() == STB_LOCAL)
        return ResolvedSymbol{this, &symbol, addressOfSymbol(symbol), true};

    ResolvedSymbol resolved = resolver(name, importedSymbolVersion(symbol_index));
    if (resolved)
        return resolved;

    /// A hidden definition present here but absent from the scope resolves to itself.
    if (symbol.isDefined())
        return ResolvedSymbol{this, &symbol, addressOfSymbol(symbol), true};

    /// An unresolved weak reference is allowed and evaluates to the null address.
    if (symbol.isWeak())
        return ResolvedSymbol{nullptr, nullptr, nullptr, true};

    const char * version = versionNameOfSymbol(symbol_index);
    throw Exception(ErrorCodes::CANNOT_DLOPEN,
        "Undefined symbol '{}'{}{} required by '{}'",
        name, version ? " version " : "", version ? version : "", file_path);
}


void LoadedLibrary::applyOneRelocation(const RelocationWithAddend & relocation, const SymbolResolver & resolver)
{
    uint32_t type = relocation.type();
    auto * target = reinterpret_cast<uint64_t *>(load_bias + relocation.offset);

    if (type == RelocationType::Relative)
    {
        *target = load_bias + relocation.addend;
    }
    else if (type == RelocationType::IndirectRelative)
    {
        *target = reinterpret_cast<uint64_t>(callIndirectFunctionResolver(load_bias + relocation.addend));
    }
    else if (type == RelocationType::GlobalData || type == RelocationType::JumpSlot || type == RelocationType::Direct64)
    {
        ResolvedSymbol resolved = resolveForRelocation(relocation.symbolIndex(), resolver);
        *target = reinterpret_cast<uint64_t>(resolved.address) + relocation.addend;
    }
    else if (type == RelocationType::Copy)
    {
        ResolvedSymbol resolved = resolveForRelocation(relocation.symbolIndex(), resolver);
        std::memcpy(target, resolved.address, symbol_table[relocation.symbolIndex()].size);
    }
    else if (type == RelocationType::TLSModuleID)
    {
        /// Symbol index 0 means "this module" (the local-dynamic model).
        if (relocation.symbolIndex() == 0)
            *target = tls_module_id;
        else
        {
            ResolvedSymbol resolved = resolveForRelocation(relocation.symbolIndex(), resolver);
            if (resolved.library == nullptr)
                throw Exception(ErrorCodes::CANNOT_DLOPEN,
                    "Thread-local symbol has no defining module in '{}'", file_path);
            *target = resolved.library->tls_module_id;
        }
    }
    else if (type == RelocationType::TLSModuleOffset)
    {
        if (relocation.symbolIndex() == 0)
            *target = relocation.addend;
        else
        {
            ResolvedSymbol resolved = resolveForRelocation(relocation.symbolIndex(), resolver);
            *target = (resolved.symbol ? resolved.symbol->value : 0) + relocation.addend;
        }
    }
    else if (type == RelocationType::TLSThreadPointerOffset || type == RelocationType::TLSDescriptor)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "'{}' uses initial-exec or descriptor-based thread-local storage, which this loader does not support "
            "(only the general-dynamic model is available)", file_path);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Unsupported relocation type {} in '{}'", type, file_path);
    }
}


void LoadedLibrary::applyCompactRelativeRelocations()
{
    /// The RELR table is a compact encoding of a long run of R_*_RELATIVE relocations. Each entry is either an
    /// address (low bit clear) that starts a new run, or a bitmap (low bit set) whose bits mark the next 63
    /// pointer-sized slots to be rebased.
    uint64_t * where = nullptr;
    for (size_t i = 0; i < relr_relocations_count; ++i)
    {
        uint64_t entry = relr_relocations[i];
        if ((entry & 1) == 0)
        {
            where = reinterpret_cast<uint64_t *>(load_bias + entry);
            *where += load_bias;
            ++where;
        }
        else
        {
            uint64_t bitmap = entry >> 1;
            for (uint64_t bit = 0; bitmap != 0; bitmap >>= 1, ++bit)
                if (bitmap & 1)
                    where[bit] += load_bias;
            where += 63;
        }
    }
}


void LoadedLibrary::applyRelocations(const SymbolResolver & resolver)
{
    applyCompactRelativeRelocations();

    /// Indirect-function (IFUNC) resolvers may depend on the object being otherwise fully relocated, so apply
    /// them in a second pass after every ordinary relocation of this object.
    auto applyTable = [&](const RelocationWithAddend * table, size_t count, bool indirect_pass)
    {
        for (size_t i = 0; i < count; ++i)
        {
            bool is_indirect = table[i].type() == RelocationType::IndirectRelative;
            if (is_indirect == indirect_pass)
                applyOneRelocation(table[i], resolver);
        }
    };

    applyTable(rela_relocations, rela_relocations_count, false);
    applyTable(plt_relocations, plt_relocations_count, false);
    applyTable(rela_relocations, rela_relocations_count, true);
    applyTable(plt_relocations, plt_relocations_count, true);
}


void LoadedLibrary::protectReadOnlyRelocations()
{
    if (relro_start == nullptr || relro_size == 0)
        return;
    if (::mprotect(relro_start, relro_size, PROT_READ) != 0)
        throw Exception(ErrorCodes::CANNOT_MPROTECT, "Cannot protect the RELRO region of '{}'", file_path);
}


void LoadedLibrary::runInitializers()
{
    if (initializers_ran)
        return;
    initializers_ran = true;

    /// glibc passes (argc, argv, envp) to initializers; a plain void() initializer simply ignores them.
    using Initializer = void (*)(int, char **, char **);
    static char * empty_argv[] = {nullptr};

    if (init_function != nullptr)
        reinterpret_cast<Initializer>(init_function)(0, empty_argv, environ);

    for (auto * function : init_array)
        if (function != nullptr)
            reinterpret_cast<Initializer>(function)(0, empty_argv, environ);
}


void LoadedLibrary::runFinalizers()
{
    if (finalizers_ran || !initializers_ran)
        return;
    finalizers_ran = true;

    /// Finalizers run in the reverse of initialization order.
    for (auto iterator = fini_array.rbegin(); iterator != fini_array.rend(); ++iterator)
        if (*iterator != nullptr)
            (*iterator)();

    if (fini_function != nullptr)
        fini_function();
}

}

}
