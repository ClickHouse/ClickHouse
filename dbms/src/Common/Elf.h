#pragma once

#include <IO/MMapReadBufferFromFile.h>

#include <string>
#include <optional>
#include <functional>

#ifdef __APPLE__

#include <libelf/gelf.h>

typedef Elf64_Addr GElf_Addr;
typedef Elf64_Half GElf_Half;
typedef Elf64_Off GElf_Off;
typedef Elf64_Sword GElf_Sword;
typedef Elf64_Word GElf_Word;
typedef Elf64_Sxword GElf_Sxword;
typedef Elf64_Xword GElf_Xword;

typedef Elf64_Ehdr GElf_Ehdr;
typedef Elf64_Phdr GElf_Phdr;
typedef Elf64_Shdr GElf_Shdr;
typedef Elf64_Dyn GElf_Dyn;
typedef Elf64_Rel GElf_Rel;
typedef Elf64_Rela GElf_Rela;
typedef Elf64_Sym GElf_Sym;

#define DT_GNU_HASH	0x6ffffef5 /* GNU-style hash table. */

#define __ELF_NATIVE_CLASS __WORDSIZE
#define ElfW(type)         _ElfW(Elf, __ELF_NATIVE_CLASS, type)
#define _ElfW(e, w, t)     _ElfW_1(e, w, _##t)
#define _ElfW_1(e, w, t)   e##w##t

#else

#include <elf.h>
#include <link.h>

#endif


using ElfAddr = ElfW(Addr);
using ElfEhdr = ElfW(Ehdr);
using ElfOff = ElfW(Off);
using ElfPhdr = ElfW(Phdr);
using ElfShdr = ElfW(Shdr);
using ElfSym = ElfW(Sym);


namespace DB
{

/** Allow to navigate sections in ELF.
  */
class Elf final
{
public:
    struct Section
    {
        const ElfShdr & header;
        const char * name() const;

        const char * begin() const;
        const char * end() const;
        size_t size() const;

        Section(const ElfShdr & header_, const Elf & elf_);

    private:
        const Elf & elf;
    };

    explicit Elf(const std::string & path);

    bool iterateSections(std::function<bool(const Section & section, size_t idx)> && pred) const;
    std::optional<Section> findSection(std::function<bool(const Section & section, size_t idx)> && pred) const;
    std::optional<Section> findSectionByName(const char * name) const;

    const char * begin() const { return mapped; }
    const char * end() const { return mapped + elf_size; }
    size_t size() const { return elf_size; }

private:
    MMapReadBufferFromFile in;
    size_t elf_size;
    const char * mapped;
    const ElfEhdr * header;
    const ElfShdr * section_headers;
    const char * section_names = nullptr;
};

}
