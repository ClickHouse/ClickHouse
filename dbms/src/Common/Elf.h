#pragma once

#include <IO/MMapReadBufferFromFile.h>

#include <string>
#include <optional>
#include <functional>

#include <elf.h>
#include <link.h>


using ElfAddr = ElfW(Addr);
using ElfEhdr = ElfW(Ehdr);
using ElfOff = ElfW(Off);
using ElfPhdr = ElfW(Phdr);
using ElfShdr = ElfW(Shdr);
using ElfSym = ElfW(Sym);


namespace DB
{

class Elf
{
public:
    struct Section
    {
        const ElfShdr & header;
        const char * name() const;

        const char * begin() const;
        const char * end() const;

        Section(const ElfShdr & header, const Elf & elf);

    private:
        const Elf & elf;
    };

    Elf(const std::string & path);

    std::optional<Section> findSection(std::function<bool(const Section & section, size_t idx)> && pred) const;
    bool iterateSections(std::function<bool(const Section & section, size_t idx)> && pred) const;

    const char * end() const { return mapped + size; }

private:
    MMapReadBufferFromFile in;
    size_t size;
    const char * mapped;
    const ElfEhdr * header;
    const ElfShdr * section_headers;
    const char * section_names = nullptr;
};

}
