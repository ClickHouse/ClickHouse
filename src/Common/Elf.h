#pragma once

#if defined(__ELF__) && !defined(__FreeBSD__)

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
using ElfNhdr = ElfW(Nhdr);
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

    /// Obtain build id from SHT_NOTE of section headers (fallback to PT_NOTES section of program headers).
    /// Return empty string if does not exist.
    /// The string is returned in binary. Note that "readelf -n ./clickhouse-server" prints it in hex.
    String getBuildID() const;
    static String getBuildID(const char * nhdr_pos, size_t size);

    /// Hash of the binary for integrity checks.
    String getBinaryHash() const;

private:
    MMapReadBufferFromFile in;
    size_t elf_size;
    const char * mapped;
    const ElfEhdr * header;
    const ElfShdr * section_headers;
    const ElfPhdr * program_headers;
    const char * section_names = nullptr;
};

}

#endif
