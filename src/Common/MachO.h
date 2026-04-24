#pragma once

#if defined(OS_DARWIN)

#include <IO/MMapReadBufferFromFile.h>

#include <string>
#include <optional>
#include <vector>


namespace DB
{

/** Parse a Mach-O binary file to extract DWARF debug info sections.
  * Used to read dSYM bundles on macOS.
  */
class MachO final
{
public:
    struct Section
    {
        const char * data;
        size_t length;

        const char * begin() const { return data; }
        const char * end() const { return data + length; }
        size_t size() const { return length; }
    };

    explicit MachO(const std::string & path);

    /// Find a section by its ELF-style name (e.g. ".debug_info").
    /// Maps to Mach-O section names (e.g. "__debug_info" in "__DWARF" segment).
    std::optional<Section> findSectionByName(const char * name) const;

    const char * begin() const { return mapped; }
    size_t size() const { return file_size; }
    const std::string & getPath() const { return path; }

private:
    std::string path;
    MMapReadBufferFromFile in;
    size_t file_size;
    const char * mapped;

    struct SectionInfo
    {
        std::string name; /// Mach-O section name like "__debug_info"
        const char * data;
        size_t size;
    };
    std::vector<SectionInfo> dwarf_sections;

    void init();
};

}

#endif
