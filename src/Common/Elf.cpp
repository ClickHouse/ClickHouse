#include <Common/Elf.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ELF;
}


Elf::Elf(const std::string & path_)
{
    in.emplace(path_, 0);
    init(in->buffer().begin(), in->buffer().size(), path_);
}

Elf::Elf(const char * data, size_t size, const std::string & path_)
{
    init(data, size, path_);
}

void Elf::init(const char * data, size_t size, const std::string & path_)
{
    path = path_;
    mapped = data;
    elf_size = size;

    /// Check if it's an elf.
    if (elf_size < sizeof(ElfHeader))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The size of supposedly ELF file '{}' is too small", path);

    header = reinterpret_cast<const ElfHeader *>(mapped);

    if (memcmp(header->ident, "\x7F""ELF", 4) != 0)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The file '{}' is not ELF according to magic", path);

    /// Get section header.
    uint64_t section_header_offset = header->shoff;
    uint16_t section_header_num_entries = header->shnum;

    if (!section_header_offset
        || !section_header_num_entries
        || section_header_offset + section_header_num_entries * sizeof(ElfSectionHeader) > elf_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' is truncated (section header points after end of file)", path);

    section_headers = reinterpret_cast<const ElfSectionHeader *>(mapped + section_header_offset);

    /// The string table with section names.
    auto section_names_strtab = findSection([&](const Section & section, size_t idx)
    {
        return section.header.type == SectionHeaderType::STRTAB && header->shstrndx == idx;
    });

    if (!section_names_strtab)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' doesn't have string table with section names", path);

    uint64_t section_names_offset = section_names_strtab->header.offset;
    if (section_names_offset >= elf_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' is truncated (section names string table points after end of file)", path);

    section_names = reinterpret_cast<const char *>(mapped + section_names_offset);

    /// Get program headers

    uint64_t program_header_offset = header->phoff;
    uint16_t program_header_num_entries = header->phnum;

    if (!program_header_offset
        || !program_header_num_entries
        || program_header_offset + program_header_num_entries * sizeof(ElfProgramHeader) > elf_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' is truncated (program header points after end of file)", path);

    program_headers = reinterpret_cast<const ElfProgramHeader *>(mapped + program_header_offset);
}


Elf::Section::Section(const ElfSectionHeader & header_, const Elf & elf_)
    : header(header_), elf(elf_)
{
}


bool Elf::iterateSections(std::function<bool(const Section & section, size_t idx)> && pred) const
{
    for (size_t idx = 0; idx < header->shnum; ++idx)
    {
        Section section(section_headers[idx], *this);

        /// Sections spans after end of file.
        if (section.header.offset + section.header.size > elf_size)
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
        if (section.header.type == SectionHeaderType::NOTE)
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
    for (size_t idx = 0; idx < header->phnum; ++idx)
    {
        const ElfProgramHeader & phdr = program_headers[idx];

        if (phdr.type == ProgramHeaderType::NOTE)
            return getBuildID(mapped + phdr.offset, phdr.filesz);
    }

    return {};
}

String Elf::getBuildID(const char * nhdr_pos, size_t size)
{
    const char * nhdr_end = nhdr_pos + size;

    while (nhdr_pos < nhdr_end)
    {
        ElfNameHeader nhdr = unalignedLoad<ElfNameHeader>(nhdr_pos);

        nhdr_pos += sizeof(ElfNameHeader) + nhdr.namesz;
        if (nhdr.type == NameHeaderType::GNU_BUILD_ID)
        {
            const char * build_id = nhdr_pos;
            return {build_id, nhdr.descsz};
        }
        nhdr_pos += nhdr.descsz;
    }

    return {};
}


String Elf::getStoredBinaryHash() const
{
    if (auto section = findSectionByName(".clickhouse.hash"))
        return {section->begin(), section->end()};
    return {};
}


const char * Elf::Section::name() const
{
    if (!elf.section_names)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Section names are not initialized");

    /// TODO buffer overflow is possible, we may need to check strlen.
    return elf.section_names + header.name;
}


const char * Elf::Section::begin() const
{
    return elf.mapped + header.offset;
}

const char * Elf::Section::end() const
{
    return begin() + size();
}

size_t Elf::Section::size() const
{
    return header.size;
}

}
