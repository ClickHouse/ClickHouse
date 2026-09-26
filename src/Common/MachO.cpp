#if defined(OS_DARWIN)

#include <Common/MachO.h>
#include <Common/Exception.h>

#include <mach-o/loader.h>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ELF;
}


MachO::MachO(const std::string & path_)
    : path(path_)
    , in(path_, 0)
    , file_size(in.buffer().size())
    , mapped(in.buffer().begin())
{
    init();
}


void MachO::init()
{
    if (file_size < sizeof(mach_header_64))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF,
            "The size of Mach-O file '{}' is too small", path);

    const auto * header = reinterpret_cast<const mach_header_64 *>(mapped);

    if (header->magic != MH_MAGIC_64)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF,
            "The file '{}' is not a 64-bit Mach-O file", path);

    const char * cmd_ptr = mapped + sizeof(mach_header_64);
    for (uint32_t i = 0; i < header->ncmds; ++i)
    {
        if (cmd_ptr + sizeof(load_command) > mapped + file_size)
            break;

        const auto * cmd = reinterpret_cast<const load_command *>(cmd_ptr);

        if (cmd->cmd == LC_SEGMENT_64)
        {
            if (cmd_ptr + sizeof(segment_command_64) > mapped + file_size)
                break;

            const auto * seg = reinterpret_cast<const segment_command_64 *>(cmd_ptr);

            if (strncmp(seg->segname, "__DWARF", 16) == 0)
            {
                const auto * sect = reinterpret_cast<const section_64 *>(
                    cmd_ptr + sizeof(segment_command_64));

                for (uint32_t j = 0; j < seg->nsects; ++j)
                {
                    if (reinterpret_cast<const char *>(&sect[j + 1]) > mapped + file_size)
                        break;

                    if (sect[j].offset > file_size || sect[j].size > file_size - sect[j].offset)
                        continue;

                    SectionInfo info;
                    /// sectname is a fixed 16-byte field, not necessarily null-terminated.
                    info.name = std::string(sect[j].sectname, strnlen(sect[j].sectname, 16));
                    info.data = mapped + sect[j].offset;
                    info.size = sect[j].size;
                    dwarf_sections.push_back(std::move(info));
                }
            }
        }

        if (cmd->cmdsize < sizeof(load_command) || cmd_ptr + cmd->cmdsize > mapped + file_size)
            break;

        cmd_ptr += cmd->cmdsize;
    }
}


std::optional<MachO::Section> MachO::findSectionByName(const char * name) const
{
    /// Map ELF-style section names to Mach-O section names.
    /// Most follow the pattern ".debug_foo" -> "__debug_foo", but Mach-O section names
    /// are limited to 16 characters, so longer names need explicit mappings.
    static const std::pair<const char *, const char *> explicit_mappings[] = {
        {".debug_str_offsets", "__debug_str_offs"},
        {".debug_rnglists", "__debug_rnglists"},
        {".debug_loclists", "__debug_loclists"},
        {".debug_line_str", "__debug_line_str"},
        {".debug_addr", "__debug_addr"},
    };

    std::string macho_name;

    for (const auto & [elf_name, macho_mapped] : explicit_mappings)
    {
        if (strcmp(name, elf_name) == 0)
        {
            macho_name = macho_mapped;
            break;
        }
    }

    if (macho_name.empty())
    {
        if (name[0] == '.')
        {
            macho_name = "__";
            macho_name += (name + 1);
        }
        else
        {
            macho_name = name;
        }

        /// Truncate to 16 characters (Mach-O section name limit).
        if (macho_name.size() > 16)
            macho_name.resize(16);
    }

    for (const auto & section : dwarf_sections)
    {
        if (section.name == macho_name)
            return Section{section.data, section.size};
    }

    return std::nullopt;
}

}

#endif
