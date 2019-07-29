#include <Common/SymbolIndex.h>
#include <Common/Elf.h>
#include <common/demangle.h>

#include <algorithm>
#include <optional>

#include <link.h>

//#include <iostream>
#include <filesystem>


namespace
{

/// Notes: "PHDR" is "Program Headers".
/// To look at program headers, you can run: objdump -p ./clickhouse-server
/// Also look at: https://wiki.osdev.org/ELF
/// Also look at: man elf
/// http://www.linker-aliens.org/blogs/ali/entry/inside_elf_symbol_tables/
/// https://stackoverflow.com/questions/32088140/multiple-string-tables-in-elf-object


/// Based on the code of musl-libc and the answer of Kanalpiroge on
/// https://stackoverflow.com/questions/15779185/list-all-the-functions-symbols-on-the-fly-in-c-code-on-a-linux-architecture
void collectSymbolsFromProgramHeaders(dl_phdr_info * info, std::vector<DB::SymbolIndex::Symbol> & symbols)
{
    /* Iterate over all headers of the current shared lib
     * (first call is for the executable itself) */
    for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index)
    {
        /* Further processing is only needed if the dynamic section is reached
         */
        if (info->dlpi_phdr[header_index].p_type != PT_DYNAMIC)
            continue;

        /* Get a pointer to the first entry of the dynamic section.
         * It's address is the shared lib's address + the virtual address
         */
        const ElfW(Dyn) * dyn_begin = reinterpret_cast<const ElfW(Dyn) *>(info->dlpi_addr + info->dlpi_phdr[header_index].p_vaddr);

//        std::cerr << "dlpi_addr: " << info->dlpi_addr << "\n";

        /// For unknown reason, addresses are sometimes relative sometimes absolute.
        auto correct_address = [](ElfW(Addr) base, ElfW(Addr) ptr)
        {
            return ptr > base ? ptr : base + ptr;
        };

        /* Iterate over all entries of the dynamic section until the
         * end of the symbol table is reached. This is indicated by
         * an entry with d_tag == DT_NULL.
         */

/*        for (auto it = dyn_begin; it->d_tag != DT_NULL; ++it)
            std::cerr << it->d_tag << "\n";*/

        size_t sym_cnt = 0;
        for (auto it = dyn_begin; it->d_tag != DT_NULL; ++it)
        {
            if (it->d_tag == DT_HASH)
            {
                const ElfW(Word) * hash = reinterpret_cast<const ElfW(Word) *>(correct_address(info->dlpi_addr, it->d_un.d_ptr));

//                std::cerr << it->d_un.d_ptr << ", " << it->d_un.d_val << "\n";

                sym_cnt = hash[1];
                break;
            }
            else if (it->d_tag == DT_GNU_HASH)
            {
//                std::cerr << it->d_un.d_ptr << ", " << it->d_un.d_val << "\n";

                /// This code based on Musl-libc.

                const uint32_t * buckets = nullptr;
                const uint32_t * hashval = nullptr;

                const ElfW(Word) * hash = reinterpret_cast<const ElfW(Word) *>(correct_address(info->dlpi_addr, it->d_un.d_ptr));

                buckets = hash + 4 + (hash[2] * sizeof(size_t) / 4);

                for (ElfW(Word) i = 0; i < hash[0]; ++i)
                    if (buckets[i] > sym_cnt)
                        sym_cnt = buckets[i];

                if (sym_cnt)
                {
                    sym_cnt -= hash[1];
                    hashval = buckets + hash[0] + sym_cnt;
                    do
                    {
                        ++sym_cnt;
                    }
                    while (!(*hashval++ & 1));
                }

                break;
            }
        }

//        std::cerr << "sym_cnt: " << sym_cnt << "\n";
        if (!sym_cnt)
            continue;

        const char * strtab = nullptr;
        for (auto it = dyn_begin; it->d_tag != DT_NULL; ++it)
        {
            if (it->d_tag == DT_STRTAB)
            {
                strtab = reinterpret_cast<const char *>(correct_address(info->dlpi_addr, it->d_un.d_ptr));
                break;
            }
        }

        if (!strtab)
            continue;

//        std::cerr << "Having strtab" << "\n";

        for (auto it = dyn_begin; it->d_tag != DT_NULL; ++it)
        {
            if (it->d_tag == DT_SYMTAB)
            {
                /* Get the pointer to the first entry of the symbol table */
                const ElfW(Sym) * elf_sym = reinterpret_cast<const ElfW(Sym) *>(correct_address(info->dlpi_addr, it->d_un.d_ptr));

                /* Iterate over the symbol table */
                for (ElfW(Word) sym_index = 0; sym_index < sym_cnt; ++sym_index)
                {
                    /// We are not interested in empty symbols.
                    if (!elf_sym[sym_index].st_size)
                        continue;

                    /* Get the name of the sym_index-th symbol.
                     * This is located at the address of st_name relative to the beginning of the string table.
                     */
                    const char * sym_name = &strtab[elf_sym[sym_index].st_name];

                    if (!sym_name)
                        continue;

//                    std::cerr << sym_name << "\n";

                    DB::SymbolIndex::Symbol symbol;
                    symbol.address_begin = reinterpret_cast<const void *>(info->dlpi_addr + elf_sym[sym_index].st_value);
                    symbol.address_end = reinterpret_cast<const void *>(info->dlpi_addr + elf_sym[sym_index].st_value + elf_sym[sym_index].st_size);
                    int unused = 0;
                    symbol.name = demangle(sym_name, unused);
                    symbol.object = info->dlpi_name;

                    symbols.push_back(std::move(symbol));
                }

                break;
            }
        }
    }
}


void collectSymbolsFromELFSymbolTable(
    dl_phdr_info * info,
    const DB::Elf & elf,
    const DB::Elf::Section & symbol_table,
    const DB::Elf::Section & string_table,
    std::vector<DB::SymbolIndex::Symbol> & symbols)
{
    /// Iterate symbol table.
    const ElfSym * symbol_table_entry = reinterpret_cast<const ElfSym *>(symbol_table.begin());
    const ElfSym * symbol_table_end = reinterpret_cast<const ElfSym *>(symbol_table.end());

    const char * strings = string_table.begin();

    for (; symbol_table_entry < symbol_table_end; ++symbol_table_entry)
    {
        if (!symbol_table_entry->st_name
            || !symbol_table_entry->st_value
            || !symbol_table_entry->st_size
            || strings + symbol_table_entry->st_name >= elf.end())
            continue;

        /// Find the name in strings table.
        const char * symbol_name = strings + symbol_table_entry->st_name;

        DB::SymbolIndex::Symbol symbol;
        symbol.address_begin = reinterpret_cast<const void *>(info->dlpi_addr + symbol_table_entry->st_value);
        symbol.address_end = reinterpret_cast<const void *>(info->dlpi_addr + symbol_table_entry->st_value + symbol_table_entry->st_size);
        int unused = 0;
        symbol.name = demangle(symbol_name, unused);
        symbol.object = info->dlpi_name;

        symbols.push_back(std::move(symbol));
    }
}


bool searchAndCollectSymbolsFromELFSymbolTable(
    dl_phdr_info * info,
    const DB::Elf & elf,
    unsigned section_header_type,
    const char * string_table_name,
    std::vector<DB::SymbolIndex::Symbol> & symbols)
{
    std::optional<DB::Elf::Section> symbol_table;
    std::optional<DB::Elf::Section> string_table;

    if (!elf.iterateSections([&](const DB::Elf::Section & section, size_t)
        {
            if (section.header.sh_type == section_header_type)
                symbol_table.emplace(section);
            else if (section.header.sh_type == SHT_STRTAB && 0 == strcmp(section.name(), string_table_name))
                string_table.emplace(section);

            if (symbol_table && string_table)
                return true;
            return false;
        }))
    {
        return false;
    }

    collectSymbolsFromELFSymbolTable(info, elf, *symbol_table, *string_table, symbols);
    return true;
}


void collectSymbolsFromELF(dl_phdr_info * info, std::vector<DB::SymbolIndex::Symbol> & symbols)
{
    std::string object_name = info->dlpi_name;

    /// If the name is empty - it's main executable.
    /// Find a elf file for the main executable.

    if (object_name.empty())
        object_name = "/proc/self/exe";

    std::error_code ec;
    object_name = std::filesystem::canonical(object_name, ec);

    if (ec)
        return;

    DB::Elf elf(object_name);

    searchAndCollectSymbolsFromELFSymbolTable(info, elf, SHT_SYMTAB, ".strtab", symbols);
    searchAndCollectSymbolsFromELFSymbolTable(info, elf, SHT_DYNSYM, ".dynstr", symbols);
}


/* Callback for dl_iterate_phdr.
 * Is called by dl_iterate_phdr for every loaded shared lib until something
 * else than 0 is returned by one call of this function.
 */
int collectSymbols(dl_phdr_info * info, size_t, void * out_symbols)
{
    /* ElfW is a macro that creates proper typenames for the used system architecture
     * (e.g. on a 32 bit system, ElfW(Dyn*) becomes "Elf32_Dyn*")
     */

    std::vector<DB::SymbolIndex::Symbol> & symbols = *reinterpret_cast<std::vector<DB::SymbolIndex::Symbol> *>(out_symbols);

    collectSymbolsFromProgramHeaders(info, symbols);
    collectSymbolsFromELF(info, symbols);

    /* Continue iterations */
    return 0;
}

}


namespace DB
{

void SymbolIndex::update()
{
    dl_iterate_phdr(collectSymbols, &symbols);
    std::sort(symbols.begin(), symbols.end());
}

const SymbolIndex::Symbol * SymbolIndex::find(const void * address) const
{
    /// First range that has left boundary greater than address.

//    std::cerr << "Searching " << address << "\n";

    auto it = std::lower_bound(symbols.begin(), symbols.end(), address);
    if (it == symbols.begin())
        return nullptr;
    else
        --it; /// Last range that has left boundary less or equals than address.

//    std::cerr << "Range: " << it->address_begin << " ... " << it->address_end << "\n";

    if (address >= it->address_begin && address < it->address_end)
        return &*it;
    else
        return nullptr;
}

}
