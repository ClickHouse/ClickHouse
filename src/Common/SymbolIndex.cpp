#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/SymbolIndex.h>
#include <Common/MemorySanitizer.h>
#include <base/hex.h>
#include <base/sort.h>

#include <algorithm>
#include <optional>
#include <cassert>

#include <link.h>

#include <filesystem>

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

*/


namespace DB
{

namespace
{

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
    dl_phdr_info * info,
    std::vector<SymbolIndex::Symbol> & symbols)
{
    /* Iterate over all headers of the current shared lib
     * (first call is for the executable itself)
     */
    __msan_unpoison(&info->dlpi_phnum, sizeof(info->dlpi_phnum));
    __msan_unpoison(&info->dlpi_phdr, sizeof(info->dlpi_phdr));
    for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index)
    {
        /* Further processing is only needed if the dynamic section is reached
         */
        __msan_unpoison(&info->dlpi_phdr[header_index], sizeof(info->dlpi_phdr[header_index]));
        if (info->dlpi_phdr[header_index].p_type != PT_DYNAMIC)
            continue;

        /* Get a pointer to the first entry of the dynamic section.
         * It's address is the shared lib's address + the virtual address
         */
        const ElfW(Dyn) * dyn_begin = reinterpret_cast<const ElfW(Dyn) *>(info->dlpi_addr + info->dlpi_phdr[header_index].p_vaddr);
        __msan_unpoison(&dyn_begin, sizeof(dyn_begin));

        /// For unknown reason, addresses are sometimes relative sometimes absolute.
        auto correct_address = [](ElfW(Addr) base, ElfW(Addr) ptr)
        {
            return ptr > base ? ptr : base + ptr;
        };

        /* Iterate over all entries of the dynamic section until the
         * end of the symbol table is reached. This is indicated by
         * an entry with d_tag == DT_NULL.
         */

        size_t sym_cnt = 0;
        {
            const auto * it = dyn_begin;
            while (true)
            {
                __msan_unpoison(it, sizeof(*it));
                if (it->d_tag != DT_NULL)
                    break;

                ElfW(Addr) base_address = correct_address(info->dlpi_addr, it->d_un.d_ptr);

                if (it->d_tag == DT_GNU_HASH)
                {
                    /// This code based on Musl-libc.

                    const uint32_t * buckets = nullptr;
                    const uint32_t * hashval = nullptr;

                    const ElfW(Word) * hash = reinterpret_cast<const ElfW(Word) *>(base_address);

                    __msan_unpoison(&hash[0], sizeof(*hash));
                    __msan_unpoison(&hash[1], sizeof(*hash));
                    __msan_unpoison(&hash[2], sizeof(*hash));

                    buckets = hash + 4 + (hash[2] * sizeof(size_t) / 4);

                    __msan_unpoison(buckets, hash[0] * sizeof(buckets[0]));

                    for (ElfW(Word) i = 0; i < hash[0]; ++i)
                        sym_cnt = std::max<size_t>(sym_cnt, buckets[i]);

                    if (sym_cnt)
                    {
                        sym_cnt -= hash[1];
                        hashval = buckets + hash[0] + sym_cnt;
                        __msan_unpoison(&hashval, sizeof(hashval));
                        do
                        {
                            ++sym_cnt;
                        }
                        while (!(*hashval++ & 1));
                    }

                    break;
                }

                ++it;
            }
        }

        if (!sym_cnt)
            continue;

        const char * strtab = nullptr;
        for (const auto * it = dyn_begin; it->d_tag != DT_NULL; ++it)
        {
            ElfW(Addr) base_address = correct_address(info->dlpi_addr, it->d_un.d_ptr);

            if (it->d_tag == DT_STRTAB)
            {
                strtab = reinterpret_cast<const char *>(base_address);
                break;
            }
        }

        if (!strtab)
            continue;

        for (const auto * it = dyn_begin; it->d_tag != DT_NULL; ++it)
        {
            ElfW(Addr) base_address = correct_address(info->dlpi_addr, it->d_un.d_ptr);

            if (it->d_tag == DT_SYMTAB)
            {
                /* Get the pointer to the first entry of the symbol table */
                const ElfW(Sym) * elf_sym = reinterpret_cast<const ElfW(Sym) *>(base_address);

                __msan_unpoison(elf_sym, sym_cnt * sizeof(*elf_sym));

                /* Iterate over the symbol table */
                for (ElfW(Word) sym_index = 0; sym_index < ElfW(Word)(sym_cnt); ++sym_index)
                {
                    /* Get the name of the sym_index-th symbol.
                     * This is located at the address of st_name relative to the beginning of the string table.
                     */
                    const char * sym_name = &strtab[elf_sym[sym_index].st_name];
                    __msan_unpoison_string(sym_name);

                    if (!sym_name)
                        continue;

                    SymbolIndex::Symbol symbol;
                    symbol.address_begin = reinterpret_cast<const void *>(
                        info->dlpi_addr + elf_sym[sym_index].st_value);
                    symbol.address_end = reinterpret_cast<const void *>(
                        info->dlpi_addr + elf_sym[sym_index].st_value + elf_sym[sym_index].st_size);
                    symbol.name = sym_name;

                    /// We are not interested in empty symbols.
                    if (elf_sym[sym_index].st_size)
                        symbols.push_back(symbol);
                }

                break;
            }
        }
    }
}


#if !defined USE_MUSL
String getBuildIDFromProgramHeaders(dl_phdr_info * info)
{
    __msan_unpoison(&info->dlpi_phnum, sizeof(info->dlpi_phnum));
    __msan_unpoison(&info->dlpi_phdr, sizeof(info->dlpi_phdr));
    for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index)
    {
        const ElfPhdr & phdr = info->dlpi_phdr[header_index];
        __msan_unpoison(&phdr, sizeof(phdr));
        if (phdr.p_type != PT_NOTE)
            continue;

        std::string_view view(reinterpret_cast<const char *>(info->dlpi_addr + phdr.p_vaddr), phdr.p_memsz);
        __msan_unpoison(view.data(), view.size());
        return Elf::getBuildID(view.data(), view.size());
    }
    return {};
}
#endif


void collectSymbolsFromELFSymbolTable(
    dl_phdr_info * info,
    const Elf & elf,
    const Elf::Section & symbol_table,
    const Elf::Section & string_table,
    std::vector<SymbolIndex::Symbol> & symbols)
{
    /// Iterate symbol table.
    const ElfSym * symbol_table_entry = reinterpret_cast<const ElfSym *>(symbol_table.begin());
    const ElfSym * symbol_table_end = reinterpret_cast<const ElfSym *>(symbol_table.end());

    const char * strings = string_table.begin();

    for (; symbol_table_entry < symbol_table_end; ++symbol_table_entry)
    {
        if (!symbol_table_entry->st_name
            || !symbol_table_entry->st_value
            || strings + symbol_table_entry->st_name >= elf.end())
            continue;

        /// Find the name in strings table.
        const char * symbol_name = strings + symbol_table_entry->st_name;

        if (!symbol_name)
            continue;

        SymbolIndex::Symbol symbol;
        symbol.address_begin = reinterpret_cast<const void *>(
            info->dlpi_addr + symbol_table_entry->st_value);
        symbol.address_end = reinterpret_cast<const void *>(
            info->dlpi_addr + symbol_table_entry->st_value + symbol_table_entry->st_size);
        symbol.name = symbol_name;

        if (symbol_table_entry->st_size)
            symbols.push_back(symbol);
    }
}


bool searchAndCollectSymbolsFromELFSymbolTable(
    dl_phdr_info * info,
    const Elf & elf,
    unsigned section_header_type,
    const char * string_table_name,
    std::vector<SymbolIndex::Symbol> & symbols)
{
    std::optional<Elf::Section> symbol_table;
    std::optional<Elf::Section> string_table;

    if (!elf.iterateSections([&](const Elf::Section & section, size_t)
        {
            if (section.header.sh_type == section_header_type)
                symbol_table.emplace(section);
            else if (section.header.sh_type == SHT_STRTAB && 0 == strcmp(section.name(), string_table_name))
                string_table.emplace(section);

            return (symbol_table && string_table);
        }))
    {
        return false;
    }

    collectSymbolsFromELFSymbolTable(info, elf, *symbol_table, *string_table, symbols);
    return true;
}


void collectSymbolsFromELF(
    dl_phdr_info * info,
    std::vector<SymbolIndex::Symbol> & symbols,
    std::vector<SymbolIndex::Object> & objects,
    String & build_id)
{
    String object_name;
    String our_build_id;

#if defined (USE_MUSL)
    object_name = "/proc/self/exe";
    our_build_id = Elf(object_name).getBuildID();
    build_id = our_build_id;
#else
    /// MSan does not know that the program segments in memory are initialized.
    __msan_unpoison(info, sizeof(*info));
    __msan_unpoison_string(info->dlpi_name);

    object_name = info->dlpi_name;
    our_build_id = getBuildIDFromProgramHeaders(info);

    /// If the name is empty and there is a non-empty build-id - it's main executable.
    /// Find a elf file for the main executable and set the build-id.
    if (object_name.empty())
    {
        object_name = "/proc/self/exe";

        if (our_build_id.empty())
            our_build_id = Elf(object_name).getBuildID();

        if (build_id.empty())
            build_id = our_build_id;
    }
#endif

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

    if (our_build_id != file_build_id)
    {
        /// If debug info doesn't correspond to our binary, fallback to the info in our binary.
        if (object_name != canonical_path)
        {
            object_name = canonical_path;
            object.elf = std::make_unique<Elf>(object_name);

            /// But it can still be outdated, for example, if executable file was deleted from filesystem and replaced by another file.
            file_build_id = object.elf->getBuildID();
            if (our_build_id != file_build_id)
                return;
        }
        else
            return;
    }

    object.address_begin = reinterpret_cast<const void *>(info->dlpi_addr);
    object.address_end = reinterpret_cast<const void *>(info->dlpi_addr + object.elf->size());
    object.name = object_name;
    objects.push_back(std::move(object));

    searchAndCollectSymbolsFromELFSymbolTable(info, *objects.back().elf, SHT_SYMTAB, ".strtab", symbols);

    /// Unneeded if they were parsed from "program headers" of loaded objects.
#if defined USE_MUSL
    searchAndCollectSymbolsFromELFSymbolTable(info, *objects.back().elf, SHT_DYNSYM, ".dynstr", symbols);
#endif
}


/* Callback for dl_iterate_phdr.
 * Is called by dl_iterate_phdr for every loaded shared lib until something
 * else than 0 is returned by one call of this function.
 */
int collectSymbols(dl_phdr_info * info, size_t, void * data_ptr)
{
    SymbolIndex::Data & data = *reinterpret_cast<SymbolIndex::Data *>(data_ptr);

    collectSymbolsFromProgramHeaders(info, data.symbols);
    collectSymbolsFromELF(info, data.symbols, data.objects, data.build_id);

    /* Continue iterations */
    return 0;
}


template <typename T>
const T * find(const void * address, const std::vector<T> & vec)
{
    /// First range that has left boundary greater than address.

    auto it = std::lower_bound(vec.begin(), vec.end(), address,
        [](const T & symbol, const void * addr) { return symbol.address_begin <= addr; });

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
    dl_iterate_phdr(collectSymbols, &data);

    ::sort(data.objects.begin(), data.objects.end(), [](const Object & a, const Object & b) { return a.address_begin < b.address_begin; });
    ::sort(data.symbols.begin(), data.symbols.end(), [](const Symbol & a, const Symbol & b) { return a.address_begin < b.address_begin; });

    /// We found symbols both from loaded program headers and from ELF symbol tables.
    data.symbols.erase(std::unique(data.symbols.begin(), data.symbols.end(), [](const Symbol & a, const Symbol & b)
    {
        return a.address_begin == b.address_begin && a.address_end == b.address_end;
    }), data.symbols.end());
}

const SymbolIndex::Symbol * SymbolIndex::findSymbol(const void * address) const
{
    return find(address, data.symbols);
}

const SymbolIndex::Object * SymbolIndex::findObject(const void * address) const
{
    return find(address, data.objects);
}

String SymbolIndex::getBuildIDHex() const
{
    String build_id_binary = getBuildID();
    String build_id_hex;
    build_id_hex.resize(build_id_binary.size() * 2);

    char * pos = build_id_hex.data();
    for (auto c : build_id_binary)
    {
        writeHexByteUppercase(c, pos);
        pos += 2;
    }

    return build_id_hex;
}

const SymbolIndex & SymbolIndex::instance()
{
    static SymbolIndex instance;
    return instance;
}

}

#endif
