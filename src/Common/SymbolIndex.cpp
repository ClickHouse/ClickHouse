#include <base/defines.h>

#include <base/MemorySanitizer.h>
#include <base/hex.h>
#include <base/sort.h>
#if defined(__ELF__)
#include <Common/CompactSymbols.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#endif
#include <Common/MemoryTrackerUntrackedAllocationsBlockerInThread.h>
#include <Common/SymbolIndex.h>

#include <algorithm>
#include <cstring>
#include <new>
#include <numeric>
#include <optional>
#include <stdexcept>

#include <filesystem>

#if defined(OS_DARWIN)
#include <Common/MachO.h>
#include <mach-o/loader.h>
#include <mach-o/nlist.h>
#include <mach-o/dyld.h>
#include <cstring>
#endif

/**

ELF object can contain three different places with symbol names and addresses:

1. Symbol table in section headers. It is used for static linking and usually left in executable.
It is not loaded in memory and they are not necessary for program to run.
It does not relate to debug info and present regardless to -g flag.
You can use strip to get rid of this symbol table.
If you have this symbol table in your binary, you can manually read it and get symbol names, even for symbols from anonymous namespaces.

2. Hashes in program headers such as DT_HASH and DT_GNU_HASH.
It is necessary for dynamic object (.so libraries and any dynamically linked executable that depend on .so libraries)
because it is used for dynamic linking that happens in runtime and performed by dynamic loader.
Only exported symbols will be presented in that hash tables. Symbols from anonymous namespaces are not.
This part of executable binary is loaded in memory and accessible via 'dl_iterate_phdr', 'dladdr' and 'backtrace_symbols' functions from libc.
ClickHouse versions prior to 19.13 has used just these symbol names to symbolize stack traces
and stack traces may be incomplete due to lack of symbols with internal linkage.
But because ClickHouse is linked with most of the symbols exported (-rdynamic flag) it can still provide good enough stack traces.

3. DWARF debug info. It contains the most detailed information about symbols and everything else.
It allows to get source file names and line numbers from addresses. Only available if you use -g option for compiler.
It is also used by default for ClickHouse builds, but because of its weight (about two gigabytes)
it is split to separate binary and provided in clickhouse-common-static-dbg package.
This separate binary is placed in /usr/lib/debug/usr/bin/clickhouse.debug and is loaded automatically by tools like gdb, addr2line.
When you build ClickHouse by yourself, debug info is not split and present in a single huge binary.

What ClickHouse is using to provide good stack traces?

In versions prior to 19.13, only "program headers" (2) was used.

In version 19.13, ClickHouse will read program headers (2) and cache them,
also it will read itself as ELF binary and extract symbol tables from section headers (1)
to also symbolize functions that are not exported for dynamic linking.
And finally, it will read DWARF info (3) if available to display file names and line numbers.

What detail can you obtain depending on your binary?

If you have debug info (you build ClickHouse by yourself or install clickhouse-common-static-dbg package), you will get source file names and line numbers.
Otherwise you will get only symbol names. If your binary contains symbol table in section headers (the default, unless stripped), you will get all symbol names.
Otherwise you will get only exported symbols from program headers.

On macOS (Mach-O format), the symbol table is accessed via LC_SYMTAB load command.
The __LINKEDIT segment contains both the symbol table (nlist_64 entries) and the string table.
Images are enumerated via _dyld_image_count / _dyld_get_image_header / _dyld_get_image_vmaddr_slide.
Build ID equivalent is LC_UUID (16-byte UUID).

*/

#if defined(__ELF__)

extern "C" struct dl_phdr_info
{
    uint64_t addr;
    const char * name;
    const ElfProgramHeader * phdr;
    uint16_t phnum;
    uint64_t adds;
    uint64_t subs;
    size_t tls_modid;
    void * tls_data;
};

using DynamicLinkingProgramHeaderInfo = dl_phdr_info;

extern "C" int dl_iterate_phdr(int (*)(DynamicLinkingProgramHeaderInfo *, size_t, void *), void *);

#endif


namespace DB
{

#if defined(__ELF__)
class CompactSymbolTable
{
public:
    explicit CompactSymbolTable(std::string_view data)
        : reader(data)
        , maximum_name_granule_size(reader.maximumNameGranuleSize())
    {
    }

    CompactSymbols::Reader reader;
    size_t maximum_name_granule_size;
};
#endif

namespace
{

#if defined(__ELF__)
ZstdDCtxPtr createDecompressionContext()
{
    ZstdDCtxPtr context(ZSTD_createDCtx());
    if (!context)
        throw std::bad_alloc();
    return context;
}

std::string_view decodeCompactName(
    const CompactSymbols::Reader & reader,
    uint32_t granule_index,
    uint32_t entry_index,
    ZSTD_DCtx * decompression_context,
    std::span<char> granule_buffer,
    std::optional<CompactSymbols::NameGranuleDecoder> & decoder,
    std::string & name_buffer,
    uint32_t & cached_granule_index,
    uint32_t & cached_entry_index)
{
    constexpr uint32_t no_index = std::numeric_limits<uint32_t>::max();

    if (granule_index != cached_granule_index)
    {
        decoder.emplace(reader.decodeNameGranule(granule_index, decompression_context, granule_buffer));
        cached_granule_index = granule_index;
        cached_entry_index = no_index;
        name_buffer.clear();
    }
    else if (cached_entry_index != no_index && cached_entry_index > entry_index)
    {
        decoder->reset();
        cached_entry_index = no_index;
        name_buffer.clear();
    }

    if (cached_entry_index == entry_index)
        return std::string_view(name_buffer.data(), name_buffer.size());

    uint32_t next_entry_index = cached_entry_index == no_index ? 0 : cached_entry_index + 1;
    while (next_entry_index <= entry_index)
    {
        if (!decoder->next(name_buffer))
            return std::string_view("");
        cached_entry_index = next_entry_index;
        ++next_entry_index;
    }
    return std::string_view(name_buffer.data(), name_buffer.size());
}

class CompactNameWorkspace
{
public:
    CompactNameWorkspace()
        : decompression_context(createDecompressionContext())
    {
    }

    void prepare(size_t maximum_granule_size)
    {
        if (granule_buffer.size() < maximum_granule_size || name_buffer.capacity() < maximum_granule_size)
        {
            invalidate();
            granule_buffer.resize(maximum_granule_size);
            name_buffer.reserve(maximum_granule_size);
        }
    }

    void invalidate()
    {
        source_id = std::numeric_limits<uint32_t>::max();
        granule_index = std::numeric_limits<uint32_t>::max();
        entry_index = std::numeric_limits<uint32_t>::max();
        decoder.reset();
        name_buffer.clear();
    }

    ZstdDCtxPtr decompression_context;
    std::vector<char> granule_buffer;
    std::string name_buffer;
    std::optional<CompactSymbols::NameGranuleDecoder> decoder;
    uint32_t source_id = std::numeric_limits<uint32_t>::max();
    uint32_t granule_index = std::numeric_limits<uint32_t>::max();
    uint32_t entry_index = std::numeric_limits<uint32_t>::max();
};

CompactNameWorkspace & compactNameWorkspace()
{
    thread_local CompactNameWorkspace workspace;
    return workspace;
}

CompactNameWorkspace & preparedCompactNameWorkspace(size_t maximum_granule_size)
{
    /// Account the thread-lifetime workspace globally, not to the query that first needs symbolization.
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    CompactNameWorkspace & workspace = compactNameWorkspace();
    workspace.prepare(maximum_granule_size);
    return workspace;
}

/// Notes: "PHDR" is "Program Headers".
/// To look at program headers, run:
///  readelf -l ./clickhouse-server
/// To look at section headers, run:
///  readelf -S ./clickhouse-server
/// Also look at: https://wiki.osdev.org/ELF
/// Also look at: man elf
/// http://www.linker-aliens.org/blogs/ali/entry/inside_elf_symbol_tables/
/// https://stackoverflow.com/questions/32088140/multiple-string-tables-in-elf-object


/// Based on the code of musl-libc and the answer of Kanalpiroge on
/// https://stackoverflow.com/questions/15779185/list-all-the-functions-symbols-on-the-fly-in-c-code-on-a-linux-architecture
/// It does not extract all the symbols (but only public - exported and used for dynamic linking),
/// but will work if we cannot find or parse ELF files.
void collectSymbolsFromProgramHeaders(
    DynamicLinkingProgramHeaderInfo * info,
    SymbolIndex::Data & data)
{
    /* Iterate over all headers of the current shared lib
     * (first call is for the executable itself)
     */
    __msan_unpoison(&info->addr, sizeof(info->addr));
    __msan_unpoison(&info->phnum, sizeof(info->phnum));
    __msan_unpoison(&info->phdr, sizeof(info->phdr));
    for (size_t header_index = 0; header_index < info->phnum; ++header_index)
    {
        /* Further processing is only needed if the dynamic section is reached
         */
        __msan_unpoison(&info->phdr[header_index], sizeof(info->phdr[header_index]));
        if (info->phdr[header_index].type != ProgramHeaderType::DYNAMIC)
            continue;

        /* Get a pointer to the first entry of the dynamic section.
         * It's address is the shared lib's address + the virtual address
         */
        const ElfDyn * dyn_begin = reinterpret_cast<const ElfDyn *>(info->addr + info->phdr[header_index].vaddr);
        __msan_unpoison(&dyn_begin, sizeof(dyn_begin));

        /// For unknown reason, addresses are sometimes relative sometimes absolute.
        auto correct_address = [](uint64_t base, uint64_t ptr)
        {
            return ptr > base ? ptr : base + ptr;
        };

        /* Iterate over all entries of the dynamic section until the
         * end of the symbol table is reached. This is indicated by
         * an entry with d_tag == ProgramHeaderType::Null.
         */

        size_t sym_cnt = 0;
        {
            for (const auto * it = dyn_begin; ; ++it)
            {
                __msan_unpoison(it, sizeof(*it));

                if (it->tag == DynamicTableTag::Null)
                    break;

                if (it->tag != DynamicTableTag::GNU_HASH)
                    continue;

                uint64_t base_address = correct_address(info->addr, it->ptr);

                /// This code based on Musl-libc.

                const uint32_t * buckets = nullptr;
                const uint32_t * hashval = nullptr;

                const uint32_t * hash = reinterpret_cast<const uint32_t *>(base_address);

                /// Unpoison the GNU hash table header (4 uint32_t values: nbuckets, symoffset, bloom_size, maskwords)
                __msan_unpoison(hash, 4 * sizeof(uint32_t));

                buckets = hash + 4 + (hash[2] * sizeof(size_t) / 4);

                __msan_unpoison(buckets, hash[0] * sizeof(buckets[0]));

                for (uint32_t i = 0; i < hash[0]; ++i)
                    sym_cnt = std::max<size_t>(sym_cnt, buckets[i]);

                if (sym_cnt)
                {
                    sym_cnt -= hash[1];
                    hashval = buckets + hash[0] + sym_cnt;

                    do
                    {
                        __msan_unpoison(hashval, sizeof(*hashval));
                        ++sym_cnt;
                    }
                    while (!(*hashval++ & 1));
                }

                break;
            }
        }

        if (!sym_cnt)
            continue;

        const char * strtab = nullptr;
        for (const auto * it = dyn_begin; it->tag != DynamicTableTag::Null; ++it)
        {
            uint64_t base_address = correct_address(info->addr, it->ptr);

            if (it->tag == DynamicTableTag::STRTAB)
            {
                strtab = reinterpret_cast<const char *>(base_address);
                break;
            }
        }

        if (!strtab)
            continue;

        for (const auto * it = dyn_begin; it->tag != DynamicTableTag::Null; ++it)
        {
            uint64_t base_address = correct_address(info->addr, it->ptr);

            if (it->tag == DynamicTableTag::SYMTAB)
            {
                /* Get the pointer to the first entry of the symbol table */
                const ElfSymbol * elf_sym = reinterpret_cast<const ElfSymbol *>(base_address);
                uint32_t source_id = data.registerNameSource(SymbolIndex::NameSource::direct(strtab));

                __msan_unpoison(elf_sym, sym_cnt * sizeof(*elf_sym));

                /* Iterate over the symbol table */
                for (uint32_t sym_index = 0; sym_index < uint32_t(sym_cnt); ++sym_index)
                {
                    /* Get the name of the sym_index-th symbol.
                     * This is located at the address of st_name relative to the beginning of the string table.
                     */
                    const char * sym_name = &strtab[elf_sym[sym_index].name];
                    __msan_unpoison_string(sym_name);

                    if (!sym_name)
                        continue;

                    SymbolIndex::Symbol symbol{};
                    symbol.offset_begin = reinterpret_cast<const void *>(
                        elf_sym[sym_index].value);
                    symbol.offset_end = reinterpret_cast<const void *>(
                        elf_sym[sym_index].value + elf_sym[sym_index].size);
                    symbol.setNameReference(source_id, static_cast<uint32_t>(elf_sym[sym_index].name));

                    /// We are not interested in empty symbols.
                    if (elf_sym[sym_index].size)
                        data.symbols.push_back(symbol);
                }

                break;
            }
        }
    }
}


#if !defined USE_MUSL
String getBuildIDFromProgramHeaders(DynamicLinkingProgramHeaderInfo * info)
{
    __msan_unpoison(&info->addr, sizeof(info->addr));
    __msan_unpoison(&info->phnum, sizeof(info->phnum));
    __msan_unpoison(&info->phdr, sizeof(info->phdr));
    for (size_t header_index = 0; header_index < info->phnum; ++header_index)
    {
        const ElfProgramHeader & phdr = info->phdr[header_index];
        __msan_unpoison(&phdr, sizeof(phdr));
        if (phdr.type != ProgramHeaderType::NOTE)
            continue;

        std::string_view view(reinterpret_cast<const char *>(info->addr + phdr.vaddr), phdr.memsz);
        __msan_unpoison(view.data(), view.size());
        String build_id = Elf::getBuildID(view.data(), view.size());
        if (!build_id.empty()) // there may be multiple PT_NOTE segments
            return build_id;
    }
    return {};
}
#endif


void collectSymbolsFromELFSymbolTable(
    const Elf & elf,
    const Elf::Section & symbol_table,
    const Elf::Section & string_table,
    SymbolIndex::Data & data)
{
    /// Iterate symbol table.
    const ElfSymbol * symbol_table_entry = reinterpret_cast<const ElfSymbol *>(symbol_table.begin());
    const ElfSymbol * symbol_table_end = reinterpret_cast<const ElfSymbol *>(symbol_table.end());

    const char * strings = string_table.begin();
    uint32_t source_id = data.registerNameSource(SymbolIndex::NameSource::direct(strings));

    for (; symbol_table_entry < symbol_table_end; ++symbol_table_entry)
    {
        if (!symbol_table_entry->name
            || !symbol_table_entry->value
            || strings + symbol_table_entry->name >= elf.end())
            continue;

        /// Find the name in strings table.
        const char * symbol_name = strings + symbol_table_entry->name;

        if (!symbol_name)
            continue;

        SymbolIndex::Symbol symbol{};
        symbol.offset_begin = reinterpret_cast<const void *>(
            symbol_table_entry->value);
        symbol.offset_end = reinterpret_cast<const void *>(
            symbol_table_entry->value + symbol_table_entry->size);
        symbol.setNameReference(source_id, static_cast<uint32_t>(symbol_table_entry->name));

        if (symbol_table_entry->size)
            data.symbols.push_back(symbol);
    }
}


bool searchAndCollectSymbolsFromELFSymbolTable(
    const Elf & elf,
    SectionHeaderType section_header_type,
    const char * string_table_name,
    SymbolIndex::Data & data)
{
    std::optional<Elf::Section> symbol_table;
    std::optional<Elf::Section> string_table;

    if (!elf.iterateSections([&](const Elf::Section & section, size_t)
        {
            if (section.header.type == section_header_type)
                symbol_table.emplace(section);
            else if (section.header.type == SectionHeaderType::STRTAB && 0 == strcmp(section.name(), string_table_name))
                string_table.emplace(section);

            return (symbol_table && string_table);
        }))
    {
        return false;
    }

    collectSymbolsFromELFSymbolTable(elf, *symbol_table, *string_table, data);
    return true;
}


bool searchAndCollectCompactSymbols(const Elf & elf, SymbolIndex::Data & data)
{
    auto section = elf.findSectionByName(CompactSymbols::section_name);
    if (!section)
        return false;

    try
    {
        auto table = std::make_shared<CompactSymbolTable>(std::string_view(section->begin(), section->size()));
        auto addresses = table->reader.decodeAddresses();
        if (data.compact_symbol_tables.size() >= SymbolIndex::Symbol::invalid_source_id
            || data.name_sources.size() >= SymbolIndex::Symbol::invalid_source_id)
            return true;

        uint32_t table_index = static_cast<uint32_t>(data.compact_symbol_tables.size());
        data.compact_symbol_tables.push_back(table);
        uint32_t source_id = data.registerNameSource(SymbolIndex::NameSource::compact(table_index));
        data.symbols.reserve(data.symbols.size() + addresses.size());
        for (const auto & address : addresses)
        {
            if (!address.address || !address.size || address.size > std::numeric_limits<uint64_t>::max() - address.address)
                continue;

            SymbolIndex::Symbol symbol{};
            symbol.offset_begin = reinterpret_cast<const void *>(address.address);
            symbol.offset_end = reinterpret_cast<const void *>(address.address + address.size);
            symbol.setNameReference(source_id, address.name_index);
            data.symbols.push_back(symbol);
            data.has_compact_symbols = true;
        }
    }
    catch (const std::runtime_error &)
    {
        return false;
    }

    return true;
}


void collectSymbolsFromELF(
    DynamicLinkingProgramHeaderInfo * info,
    SymbolIndex::Data & data)
{
    String object_name;
    String build_id;

#if defined (USE_MUSL)
    object_name = "/proc/self/exe";
    build_id = Elf(object_name).getBuildID();
    data.self_build_id = build_id;
#else
    /// MSan does not know that the program segments in memory are initialized.
    __msan_unpoison(info, sizeof(*info));
    __msan_unpoison_string(info->name);

    object_name = info->name;
    build_id = getBuildIDFromProgramHeaders(info);

    /// If the name is empty and there is a non-empty build-id - it's main executable.
    /// Find a elf file for the main executable and set the build-id.
    if (object_name.empty())
    {
        object_name = "/proc/self/exe";

        if (build_id.empty())
            build_id = Elf(object_name).getBuildID();

        if (data.self_build_id.empty())
            data.self_build_id = build_id;
    }
#endif

    /// Note: we load ELF from file; this doesn't work for vdso because it's only present in memory.
    std::error_code ec;
    std::filesystem::path canonical_path = std::filesystem::canonical(object_name, ec);

    if (ec)
        return;

    /// Debug info and symbol table sections may be split to separate binary.
    std::filesystem::path local_debug_info_path = canonical_path.parent_path() / canonical_path.stem();
    local_debug_info_path += ".debug";
    std::filesystem::path debug_info_path = std::filesystem::path("/usr/lib/debug") / canonical_path.relative_path();
    debug_info_path += ".debug";

    /// NOTE: This is a workaround for current package system.
    ///
    /// Since nfpm cannot copy file only if it exists,
    /// and so in cmake empty .debug file is created instead,
    /// but if we will try to load empty Elf file, then the CANNOT_PARSE_ELF
    /// exception will be thrown from the Elf::Elf.
    auto exists_not_empty = [](const std::filesystem::path & path)
    {
        return std::filesystem::exists(path) && !std::filesystem::is_empty(path);
    };

    if (exists_not_empty(local_debug_info_path))
        object_name = local_debug_info_path;
    else if (exists_not_empty(debug_info_path))
        object_name = debug_info_path;
    else if (build_id.size() >= 2)
    {
        // Check if there is a .debug file in .build-id folder. For example:
        // /usr/lib/debug/.build-id/e4/0526a12e9a8f3819a18694f6b798f10c624d5c.debug
        String build_id_hex;
        build_id_hex.resize(build_id.size() * 2);

        char * pos = build_id_hex.data();
        for (auto c : build_id)
        {
            writeHexByteLowercase(c, pos);
            pos += 2;
        }

        std::filesystem::path build_id_debug_info_path(
            fmt::format("/usr/lib/debug/.build-id/{}/{}.debug", build_id_hex.substr(0, 2), build_id_hex.substr(2)));
        if (exists_not_empty(build_id_debug_info_path))
            object_name = build_id_debug_info_path;
        else
            object_name = canonical_path;
    }
    else
        object_name = canonical_path;

    /// But we have to compare Build ID to check that debug info corresponds to the same executable.

    SymbolIndex::Object object;
    object.elf = std::make_unique<Elf>(object_name);

    String file_build_id = object.elf->getBuildID();

    if (build_id != file_build_id)
    {
        /// If the separate debuginfo binary doesn't correspond to the loaded binary, fallback to
        /// the info in the loaded binary.
        if (object_name != canonical_path)
        {
            object_name = canonical_path;
            object.elf = std::make_unique<Elf>(object_name);

            /// But it can still be outdated, for example, if executable file was deleted from filesystem and replaced by another file.
            file_build_id = object.elf->getBuildID();
            if (build_id != file_build_id)
                return;
        }
        else
            return;
    }

    object.address_begin = reinterpret_cast<const void *>(info->addr);
    object.address_end = reinterpret_cast<const void *>(info->addr + object.elf->size());
    object.name = object_name;
    data.objects.push_back(std::move(object));

    if (!searchAndCollectCompactSymbols(*data.objects.back().elf, data))
        searchAndCollectSymbolsFromELFSymbolTable(*data.objects.back().elf, SectionHeaderType::SYMTAB, ".strtab", data);

    /// Unneeded if they were parsed from "program headers" of loaded objects.
#if defined USE_MUSL
    searchAndCollectSymbolsFromELFSymbolTable(*data.objects.back().elf, SectionHeaderType::DYNSYM, ".dynstr", data);
#endif
}


/* Callback for dl_iterate_phdr.
 * Is called by dl_iterate_phdr for every loaded shared lib until something
 * else than 0 is returned by one call of this function.
 */
int collectSymbols(DynamicLinkingProgramHeaderInfo * info, size_t, void * data_ptr)
{
    SymbolIndex::Data & data = *reinterpret_cast<SymbolIndex::Data *>(data_ptr);

    collectSymbolsFromProgramHeaders(info, data);
    collectSymbolsFromELF(info, data);

    /* Continue iterations */
    return 0;
}

#elif defined(OS_DARWIN)

/// Collect symbols from a single Mach-O image loaded in the current process.
/// Uses the in-memory __LINKEDIT segment to access the symbol table (LC_SYMTAB)
/// without opening any files.
void collectSymbolsFromMachOImage(
    uint32_t image_index,
    SymbolIndex::Data & data)
{
    const struct mach_header_64 * header
        = reinterpret_cast<const struct mach_header_64 *>(_dyld_get_image_header(image_index));
    if (!header || header->magic != MH_MAGIC_64)
        return;

    intptr_t slide = _dyld_get_image_vmaddr_slide(image_index);
    const char * image_name = _dyld_get_image_name(image_index);

    const uint8_t * cmd_ptr = reinterpret_cast<const uint8_t *>(header + 1);

    const struct symtab_command * symtab_cmd = nullptr;
    const struct segment_command_64 * linkedit_segment = nullptr;

    bool found_text = false;
    uintptr_t min_addr = UINTPTR_MAX;
    uintptr_t max_addr = 0;

    for (uint32_t i = 0; i < header->ncmds; ++i)
    {
        const struct load_command * cmd = reinterpret_cast<const struct load_command *>(cmd_ptr);

        if (cmd->cmd == LC_SEGMENT_64)
        {
            const struct segment_command_64 * seg = reinterpret_cast<const struct segment_command_64 *>(cmd);

            /// Track address range, skip __PAGEZERO (which has filesize == 0)
            if (seg->filesize > 0 && seg->vmsize > 0)
            {
                uintptr_t seg_start = seg->vmaddr + slide;
                uintptr_t seg_end = seg_start + seg->vmsize;
                min_addr = std::min(min_addr, seg_start);
                max_addr = std::max(max_addr, seg_end);
            }

            if (strcmp(seg->segname, "__LINKEDIT") == 0)
                linkedit_segment = seg;
            else if (strcmp(seg->segname, "__TEXT") == 0)
                found_text = true;
        }
        else if (cmd->cmd == LC_SYMTAB)
        {
            symtab_cmd = reinterpret_cast<const struct symtab_command *>(cmd);
        }
        else if (cmd->cmd == LC_UUID)
        {
            /// Extract build ID (LC_UUID) for the main executable (image_index == 0)
            if (image_index == 0 && data.self_build_id.empty())
            {
                /// uuid_command layout: { uint32_t cmd, uint32_t cmdsize, uint8_t uuid[16] }
                const uint8_t * uuid_bytes = cmd_ptr + 8;
                data.self_build_id.assign(reinterpret_cast<const char *>(uuid_bytes), 16);
            }
        }

        cmd_ptr += cmd->cmdsize;
    }

    if (!symtab_cmd || !linkedit_segment || !found_text)
        return;

    /// The __LINKEDIT segment is mapped into memory.
    /// Convert file offsets from LC_SYMTAB to in-memory addresses:
    ///   memory_addr = linkedit_vmaddr + slide - linkedit_fileoff + file_offset
    uintptr_t linkedit_base = linkedit_segment->vmaddr + slide - linkedit_segment->fileoff;

    const struct nlist_64 * sym_table
        = reinterpret_cast<const struct nlist_64 *>(linkedit_base + symtab_cmd->symoff);
    const char * str_table
        = reinterpret_cast<const char *>(linkedit_base + symtab_cmd->stroff);
    uint32_t source_id = data.registerNameSource(SymbolIndex::NameSource::direct(str_table));

    std::vector<SymbolIndex::Symbol> local_symbols;
    local_symbols.reserve(symtab_cmd->nsyms / 4);

    for (uint32_t j = 0; j < symtab_cmd->nsyms; ++j)
    {
        const struct nlist_64 & sym = sym_table[j];

        /// Skip debug symbols (STABS entries)
        if (sym.n_type & N_STAB)
            continue;
        /// Skip undefined symbols
        if ((sym.n_type & N_TYPE) == N_UNDF)
            continue;
        /// Skip symbols with no address
        if (sym.n_value == 0)
            continue;

        uint32_t str_index = sym.n_un.n_strx;
        if (str_index == 0 || str_index >= symtab_cmd->strsize)
            continue;

        const char * sym_name = str_table + str_index;
        if (!sym_name || sym_name[0] == '\0')
            continue;

        /// Mach-O prepends an underscore to C/C++ symbol names
        if (sym_name[0] == '_')
        {
            ++str_index;
            if (str_index >= symtab_cmd->strsize)
                continue;
            sym_name = str_table + str_index;
        }

        if (sym_name[0] == '\0')
            continue;

        SymbolIndex::Symbol symbol;
        /// On macOS, store absolute virtual addresses (n_value + slide) to avoid
        /// overlap between symbols from different objects that would have the same
        /// relative offsets. findSymbol skips the address-to-offset conversion on macOS.
        symbol.offset_begin = reinterpret_cast<const void *>(sym.n_value + slide);
        symbol.offset_end = symbol.offset_begin; /// Size will be computed below
        symbol.setNameReference(source_id, str_index);

        local_symbols.push_back(symbol);
    }

    /// Sort by address and compute sizes from gaps between consecutive symbols
    ::sort(local_symbols.begin(), local_symbols.end(),
        [](const SymbolIndex::Symbol & a, const SymbolIndex::Symbol & b)
        { return a.offset_begin < b.offset_begin; });

    /// Deduplicate symbols at the same address
    local_symbols.erase(std::unique(local_symbols.begin(), local_symbols.end(),
        [](const SymbolIndex::Symbol & a, const SymbolIndex::Symbol & b)
        { return a.offset_begin == b.offset_begin; }),
        local_symbols.end());

    /// Mach-O nlist_64 entries don't have a size field.
    /// Estimate each symbol's size as the distance to the next symbol.
    for (size_t i = 0; i + 1 < local_symbols.size(); ++i)
        local_symbols[i].offset_end = local_symbols[i + 1].offset_begin;

    /// Last symbol extends to the end of the image
    if (!local_symbols.empty() && max_addr > min_addr)
        local_symbols.back().offset_end = reinterpret_cast<const void *>(max_addr);

    data.symbols.insert(data.symbols.end(), local_symbols.begin(), local_symbols.end());

    if (min_addr < max_addr)
    {
        SymbolIndex::Object object;
        object.address_begin = reinterpret_cast<const void *>(min_addr);
        object.address_end = reinterpret_cast<const void *>(max_addr);
        object.name = image_name ? image_name : "";
        /// object.elf is null on macOS (no ELF binary)
        object.slide = static_cast<uintptr_t>(slide);

        /// Look for a dSYM bundle next to the binary.
        /// Convention: <binary>.dSYM/Contents/Resources/DWARF/<basename>
        if (!object.name.empty())
        {
            try
            {
                std::filesystem::path binary_path(object.name);
                std::string basename = binary_path.filename().string();
                std::filesystem::path dsym_path = std::filesystem::path(object.name + ".dSYM")
                    / "Contents" / "Resources" / "DWARF" / basename;

                if (std::filesystem::exists(dsym_path))
                    object.dsym = std::make_shared<MachO>(dsym_path.string());
            }
            catch (...) // Ok: dSYM lookup is best-effort, not critical
            {
            }
        }

        data.objects.push_back(std::move(object));
    }
}

#endif


const SymbolIndex::Symbol * find(const void * offset, const std::vector<SymbolIndex::Symbol> & vec)
{
    /// First range that has left boundary greater than address.

    auto it = std::lower_bound(vec.begin(), vec.end(), offset,
        [](const SymbolIndex::Symbol & symbol, const void * addr) { return symbol.offset_begin <= addr; });

    if (it == vec.begin())
        return nullptr;
    --it; /// Last range that has left boundary less or equals than address.

    if (offset >= it->offset_begin && offset < it->offset_end)
        return &*it;
    return nullptr;
}

const SymbolIndex::Object * find(const void * address, const std::vector<SymbolIndex::Object> & vec)
{
    /// First range that has left boundary greater than address.

    auto it = std::lower_bound(vec.begin(), vec.end(), address,
        [](const SymbolIndex::Object & object, const void * addr) { return object.address_begin <= addr; });

    if (it == vec.begin())
        return nullptr;
    --it; /// Last range that has left boundary less or equals than address.

    if (address >= it->address_begin && address < it->address_end)
        return &*it;
    return nullptr;
}

}


void SymbolIndex::load()
{
#if defined(__ELF__)
    dl_iterate_phdr(collectSymbols, &data);
#elif defined(OS_DARWIN)
    uint32_t image_count = _dyld_image_count();
    for (uint32_t i = 0; i < image_count; ++i)
        collectSymbolsFromMachOImage(i, data);
#endif

#if defined(__ELF__)
    for (const auto & table : data.compact_symbol_tables)
        maximum_name_granule_size = std::max(maximum_name_granule_size, table->maximum_name_granule_size);
#endif

    ::sort(data.objects.begin(), data.objects.end(), [](const Object & a, const Object & b) { return a.address_begin < b.address_begin; });
    ::sort(data.symbols.begin(), data.symbols.end(), [](const Symbol & a, const Symbol & b) { return a.offset_begin < b.offset_begin; });

    /// We found symbols both from loaded program headers and from ELF symbol tables.
    data.symbols.erase(std::unique(data.symbols.begin(), data.symbols.end(), [](const Symbol & a, const Symbol & b)
    {
        return a.offset_begin == b.offset_begin && a.offset_end == b.offset_end;
    }), data.symbols.end());

#if defined(__ELF__)
    if (data.has_compact_symbols)
    {
        if (data.symbols.size() > std::numeric_limits<uint32_t>::max())
            throw std::runtime_error("Too many symbols for SymbolIndex scan order");

        data.symbol_scan_order.resize(data.symbols.size());
        std::iota(data.symbol_scan_order.begin(), data.symbol_scan_order.end(), 0);
        std::sort(data.symbol_scan_order.begin(), data.symbol_scan_order.end(), [this](uint32_t lhs_index, uint32_t rhs_index)
        {
            const auto & lhs = data.symbols[lhs_index];
            const auto & rhs = data.symbols[rhs_index];
            bool lhs_has_compact_name = data.hasCompactName(lhs);
            bool rhs_has_compact_name = data.hasCompactName(rhs);
            if (lhs_has_compact_name != rhs_has_compact_name)
                return !lhs_has_compact_name;
            if (!lhs_has_compact_name)
                return lhs_index < rhs_index;
            if (lhs.nameSourceId() != rhs.nameSourceId())
                return lhs.nameSourceId() < rhs.nameSourceId();
            if (lhs.nameOffsetOrIndex() != rhs.nameOffsetOrIndex())
                return lhs.nameOffsetOrIndex() < rhs.nameOffsetOrIndex();
            return lhs_index < rhs_index;
        });
    }
#endif
}

const SymbolIndex::Symbol * SymbolIndex::findSymbol(const void * address) const
{
    /// On ELF: Symbols are stored as file offsets (relative to object base).
    /// Callers may pass either absolute runtime addresses OR file offsets.
    /// - Coverage passes absolute addresses
    /// - system.stack_trace (after PR #82809) already stores file offsets
    ///
    /// Strategy: Try to find containing object. If found, input is absolute address → convert.
    /// If not found, assume input is already a file offset → use directly.
    ///
    /// On macOS: Symbols are stored as absolute virtual addresses to avoid
    /// overlap between different objects. No conversion is needed.

    const void * offset = address;

#if defined(__ELF__)
    const Object * object = findObject(address);

    if (object)
    {
        /// Input is an absolute address, convert to file offset
        offset = reinterpret_cast<const void *>(
            reinterpret_cast<uintptr_t>(address) - reinterpret_cast<uintptr_t>(object->address_begin));
    }
    /// else: input is likely already a file offset, use it directly
#endif
    /// On macOS, symbols use absolute virtual addresses, so search directly.

    return find(offset, data.symbols);
}

std::string_view SymbolIndex::getSymbolName(const Symbol & symbol) const
{
    size_t name_size = 0;
    const char * name = getSymbolNameImpl(symbol, &name_size);
    return std::string_view(name, name_size);
}

const char * SymbolIndex::getSymbolNameCString(const Symbol & symbol) const
{
    return getSymbolNameImpl(symbol, nullptr);
}

const char * SymbolIndex::getSymbolNameImpl(const Symbol & symbol, size_t * name_size) const
{
    const NameSource * source = data.nameSource(symbol);
    if (!source)
    {
        if (name_size)
            *name_size = 0;
        return "";
    }

    if (source->kind == NameSource::Kind::Direct)
    {
        if (!source->direct_base)
        {
            if (name_size)
                *name_size = 0;
            return "";
        }

        const char * direct_name = source->direct_base + symbol.nameOffsetOrIndex();
        if (name_size)
            *name_size = std::strlen(direct_name);
        return direct_name;
    }

#if defined(__ELF__)
    if (source->kind != NameSource::Kind::Compact
        || source->compact_table_index >= data.compact_symbol_tables.size())
    {
        if (name_size)
            *name_size = 0;
        return "";
    }

    uint32_t source_id = symbol.nameSourceId();
    uint32_t name_index = symbol.nameOffsetOrIndex();
    uint32_t granule_index = name_index / CompactSymbols::names_per_granule;
    uint32_t entry_index = name_index % CompactSymbols::names_per_granule;
    CompactNameWorkspace & workspace = preparedCompactNameWorkspace(maximum_name_granule_size);
    if (source_id != workspace.source_id)
    {
        workspace.invalidate();
        workspace.source_id = source_id;
    }

    try
    {
        std::string_view name = decodeCompactName(
            data.compact_symbol_tables[source->compact_table_index]->reader,
            granule_index,
            entry_index,
            workspace.decompression_context.get(),
            workspace.granule_buffer,
            workspace.decoder,
            workspace.name_buffer,
            workspace.granule_index,
            workspace.entry_index);
        if (name_size)
            *name_size = name.size();
        return name.data();
    }
    catch (const std::runtime_error &)
    {
        workspace.invalidate();
        if (name_size)
            *name_size = 0;
        return "";
    }
#else
    if (name_size)
        *name_size = 0;
    return "";
#endif
}

void SymbolIndex::warmUp() const
{
#if defined(__ELF__)
    if (maximum_name_granule_size != 0)
        preparedCompactNameWorkspace(maximum_name_granule_size);
#endif
}

SymbolIndex::SymbolIterator::SymbolIterator(const SymbolIndex & index_)
    : index(index_)
{
#if defined(__ELF__)
    if (index.maximum_name_granule_size != 0)
    {
        /// Account iterator-owned symbolization workspace globally instead of charging the current query.
        MemoryTrackerBlockerInThread blocker(VariableContext::Global);
        decompression_context = createDecompressionContext();
        granule_buffer.resize(index.maximum_name_granule_size);
        name_buffer.reserve(index.maximum_name_granule_size);
    }
#endif
}

bool SymbolIndex::SymbolIterator::next(const Symbol *& symbol, std::string_view & name)
{
    size_t symbols_size = index.data.symbol_scan_order.empty() ? index.data.symbols.size() : index.data.symbol_scan_order.size();
    if (position >= symbols_size)
        return false;

    size_t symbol_index = index.data.symbol_scan_order.empty() ? position : index.data.symbol_scan_order[position];
    ++position;
    symbol = &index.data.symbols[symbol_index];
    const NameSource * source = index.data.nameSource(*symbol);
    if (!source)
    {
        name = std::string_view("");
        return true;
    }

    if (source->kind == NameSource::Kind::Direct)
    {
        name = source->direct_base
            ? std::string_view(source->direct_base + symbol->nameOffsetOrIndex())
            : std::string_view("");
        return true;
    }

#if defined(__ELF__)
    uint32_t source_id = symbol->nameSourceId();
    uint32_t name_index = symbol->nameOffsetOrIndex();
    uint32_t granule_index = name_index / CompactSymbols::names_per_granule;
    if (source->kind != NameSource::Kind::Compact
        || source->compact_table_index >= index.data.compact_symbol_tables.size())
    {
        name = std::string_view("");
        return true;
    }

    if (source_id != cached_source_id)
    {
        cached_source_id = source_id;
        cached_granule_index = std::numeric_limits<uint32_t>::max();
        cached_entry_index = std::numeric_limits<uint32_t>::max();
        decoder.reset();
        name_buffer.clear();
    }

    uint32_t entry_index = name_index % CompactSymbols::names_per_granule;
    try
    {
        name = decodeCompactName(
            index.data.compact_symbol_tables[source->compact_table_index]->reader,
            granule_index,
            entry_index,
            decompression_context.get(),
            granule_buffer,
            decoder,
            name_buffer,
            cached_granule_index,
            cached_entry_index);
    }
    catch (const std::runtime_error &)
    {
        cached_granule_index = std::numeric_limits<uint32_t>::max();
        cached_entry_index = std::numeric_limits<uint32_t>::max();
        decoder.reset();
        name_buffer.clear();
        name = std::string_view("");
    }
#else
    name = std::string_view("");
#endif
    return true;
}

const SymbolIndex::Object * SymbolIndex::findObject(const void * address) const
{
    return find(address, data.objects);
}

const SymbolIndex::Object * SymbolIndex::thisObject() const
{
    return findObject(reinterpret_cast<const void *>(+[]{}));
}

String SymbolIndex::getBuildIDHex() const
{
    String build_id_hex;
    build_id_hex.resize(data.self_build_id.size() * 2);

    char * pos = build_id_hex.data();
    for (auto c : data.self_build_id)
    {
        writeHexByteUppercase(c, pos);
        pos += 2;
    }

    return build_id_hex;
}

const SymbolIndex & SymbolIndex::instance()
{
    /// To avoid recursive initialization of SymbolIndex we need to block debug
    /// checks in MemoryTracker.
    ///
    /// Those debug checks capture the stacktrace for collecting big
    /// allocation (> 16MiB), while SymbolIndex will do
    /// ~25MiB, and so if exception will be thrown before SymbolIndex
    /// initialized (this is the case for client/local, and no, we do not want
    /// to initialize it explicitly, since this will increase startup time for
    /// the client) and later during SymbolIndex initialization it will try to
    /// initialize it one more time, and in debug build you will get pretty
    /// nice error:
    ///
    ///   __cxa_guard_acquire detected recursive initialization: do you have a function-local static variable whose initialization depends on that function
    ///
    [[maybe_unused]] MemoryTrackerUntrackedAllocationsBlockerInThread blocker;
    static SymbolIndex instance;
    return instance;
}

}
