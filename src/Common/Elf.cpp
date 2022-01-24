#if defined(__ELF__) && !defined(__FreeBSD__)

#include <Common/Elf.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

#include <string.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ELF;
}


Elf::Elf(const std::string & path)
    : in(path, 0)
{
    /// Check if it's an elf.
    elf_size = in.buffer().size();
    if (elf_size < sizeof(ElfEhdr))
        throw Exception("The size of supposedly ELF file is too small", ErrorCodes::CANNOT_PARSE_ELF);

    mapped = in.buffer().begin();
    header = reinterpret_cast<const ElfEhdr *>(mapped);

    if (memcmp(header->e_ident, "\x7F""ELF", 4) != 0)
        throw Exception("The file is not ELF according to magic", ErrorCodes::CANNOT_PARSE_ELF);

    /// Get section header.
    ElfOff section_header_offset = header->e_shoff;
    uint16_t section_header_num_entries = header->e_shnum;

    if (!section_header_offset
        || !section_header_num_entries
        || section_header_offset + section_header_num_entries * sizeof(ElfShdr) > elf_size)
        throw Exception("The ELF is truncated (section header points after end of file)", ErrorCodes::CANNOT_PARSE_ELF);

    section_headers = reinterpret_cast<const ElfShdr *>(mapped + section_header_offset);

    /// The string table with section names.
    auto section_names_strtab = findSection([&](const Section & section, size_t idx)
    {
        return section.header.sh_type == SHT_STRTAB && header->e_shstrndx == idx;
    });

    if (!section_names_strtab)
        throw Exception("The ELF doesn't have string table with section names", ErrorCodes::CANNOT_PARSE_ELF);

    ElfOff section_names_offset = section_names_strtab->header.sh_offset;
    if (section_names_offset >= elf_size)
        throw Exception("The ELF is truncated (section names string table points after end of file)", ErrorCodes::CANNOT_PARSE_ELF);

    section_names = reinterpret_cast<const char *>(mapped + section_names_offset);

    /// Get program headers

    ElfOff program_header_offset = header->e_phoff;
    uint16_t program_header_num_entries = header->e_phnum;

    if (!program_header_offset
        || !program_header_num_entries
        || program_header_offset + program_header_num_entries * sizeof(ElfPhdr) > elf_size)
        throw Exception("The ELF is truncated (program header points after end of file)", ErrorCodes::CANNOT_PARSE_ELF);

    program_headers = reinterpret_cast<const ElfPhdr *>(mapped + program_header_offset);
}


Elf::Section::Section(const ElfShdr & header_, const Elf & elf_)
    : header(header_), elf(elf_)
{
}


bool Elf::iterateSections(std::function<bool(const Section & section, size_t idx)> && pred) const
{
    for (size_t idx = 0; idx < header->e_shnum; ++idx)
    {
        Section section(section_headers[idx], *this);

        /// Sections spans after end of file.
        if (section.header.sh_offset + section.header.sh_size > elf_size)
            continue;

        if (pred(section, idx))
            return true;
    }
    return false;
}


std::optional<Elf::Section> Elf::findSection(std::function<bool(const Section & section, size_t idx)> && pred) const
{
    std::optional<Elf::Section> result;

    iterateSections([&](const Section & section, size_t idx)
    {
        if (pred(section, idx))
        {
            result.emplace(section);
            return true;
        }
        return false;
    });

    return result;
}


std::optional<Elf::Section> Elf::findSectionByName(const char * name) const
{
    return findSection([&](const Section & section, size_t) { return 0 == strcmp(name, section.name()); });
}


String Elf::getBuildID() const
{
    /// Section headers are the first choice for a debuginfo file
    if (String build_id; iterateSections([&build_id](const Section & section, size_t)
    {
        if (section.header.sh_type == SHT_NOTE)
        {
            build_id = Elf::getBuildID(section.begin(), section.size());
            if (!build_id.empty())
            {
                return true;
            }
        }
        return false;
    }))
    {
        return build_id;
    }

    /// fallback to PHDR
    for (size_t idx = 0; idx < header->e_phnum; ++idx)
    {
        const ElfPhdr & phdr = program_headers[idx];

        if (phdr.p_type == PT_NOTE)
            return getBuildID(mapped + phdr.p_offset, phdr.p_filesz);
    }

    return {};
}

#if defined(OS_SUNOS)
String Elf::getBuildID(const char * nhdr_pos, size_t size)
{
    return {};
}
#else
String Elf::getBuildID(const char * nhdr_pos, size_t size)
{
    const char * nhdr_end = nhdr_pos + size;

    while (nhdr_pos < nhdr_end)
    {
        ElfNhdr nhdr = unalignedLoad<ElfNhdr>(nhdr_pos);

        nhdr_pos += sizeof(ElfNhdr) + nhdr.n_namesz;
        if (nhdr.n_type == NT_GNU_BUILD_ID)
        {
            const char * build_id = nhdr_pos;
            return {build_id, nhdr.n_descsz};
        }
        nhdr_pos += nhdr.n_descsz;
    }

    return {};
}
#endif // OS_SUNOS


String Elf::getBinaryHash() const
{
    if (auto section = findSectionByName(".note.ClickHouse.hash"))
        return {section->begin(), section->end()};
    else
        return {};
}


const char * Elf::Section::name() const
{
    if (!elf.section_names)
        throw Exception("Section names are not initialized", ErrorCodes::CANNOT_PARSE_ELF);

    /// TODO buffer overflow is possible, we may need to check strlen.
    return elf.section_names + header.sh_name;
}


const char * Elf::Section::begin() const
{
    return elf.mapped + header.sh_offset;
}

const char * Elf::Section::end() const
{
    return begin() + size();
}

size_t Elf::Section::size() const
{
    return header.sh_size;
}

}

#endif
