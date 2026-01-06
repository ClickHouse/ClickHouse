#pragma once

#include <IO/MMapReadBufferFromFile.h>

#include <string>
#include <optional>
#include <functional>


enum class SectionHeaderType : uint32_t
{
    SYMTAB = 2,
    STRTAB = 3,
    NOTE = 7,
    DYNSYM = 11,
};

enum class ProgramHeaderType : uint32_t
{
    NOTE = 4,
    DYNAMIC = 2,
};

enum class NameHeaderType : uint32_t
{
    GNU_BUILD_ID = 3,
};

enum class DynamicTableTag : int64_t
{
    Null = 0,
    STRTAB = 5,
    SYMTAB = 6,
    GNU_HASH = 0x6ffffef5,
};

enum SectionHeaderFlag : uint64_t
{
    COMPRESSED = 1 << 11,
};

struct __attribute__((__packed__)) ElfHeader
{
    uint8_t ident[16];
    uint16_t type;
    uint16_t machine;
    uint32_t version;
    uint64_t entry;
    uint64_t phoff;
    uint64_t shoff;
    uint32_t flags;
    uint16_t ehsize;
    uint16_t phentsize;
    uint16_t phnum;
    uint16_t shentsize;
    uint16_t shnum;
    uint16_t shstrndx;
};

struct __attribute__((__packed__)) ElfSectionHeader
{
    uint32_t name;
    SectionHeaderType type;
    uint64_t flags;
    uint64_t addr;
    uint64_t offset;
    uint64_t size;
    uint32_t link;
    uint32_t info;
    uint64_t addralign;
    uint64_t entsize;
};

struct __attribute__((__packed__)) ElfProgramHeader
{
    ProgramHeaderType type;
    uint32_t flags;
    uint64_t offset;
    uint64_t vaddr;
    uint64_t paddr;
    uint64_t filesz;
    uint64_t memsz;
    uint64_t align;
};

struct __attribute__((__packed__)) ElfSymbol
{
    uint32_t name;
    uint8_t info;
    uint8_t other;
    uint16_t shndx;
    uint64_t value;
    uint64_t size;
};

struct __attribute__((__packed__)) ElfNameHeader
{
    uint32_t namesz;
    uint32_t descsz;
    NameHeaderType type;
};

struct __attribute__((__packed__)) ElfDyn
{
    DynamicTableTag tag;
    union
    {
        uint64_t val;
        uint64_t ptr;
    };
};


namespace DB
{

/** Allow to navigate sections in ELF.
  */
class Elf final
{
public:
    struct Section
    {
        const ElfSectionHeader & header;
        const char * name() const;

        const char * begin() const;
        const char * end() const;
        size_t size() const;

        Section(const ElfSectionHeader & header_, const Elf & elf_);

    private:
        const Elf & elf;
    };

    explicit Elf(const std::string & path_);
    Elf(const char * data, size_t size, const std::string & path_);

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
    String getStoredBinaryHash() const;

private:
    std::string path; // just for error messages
    std::optional<MMapReadBufferFromFile> in;
    size_t elf_size;
    const char * mapped;
    const ElfHeader * header;
    const ElfSectionHeader * section_headers;
    const ElfProgramHeader * program_headers;
    const char * section_names = nullptr;

    void init(const char * data, size_t size, const std::string & path_);
};

}
