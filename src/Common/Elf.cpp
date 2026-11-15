#include <Common/Elf.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <base/arithmeticOverflow.h>
#include <base/unaligned.h>

#include <zlib.h>
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <thread>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_ELF;
}


/// Whether [offset, offset + length) is within `total` bytes, computed so it can't overflow.
static bool rangeWithin(uint64_t offset, uint64_t length, uint64_t total)
{
    return offset <= total && length <= total - offset;
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
        || !rangeWithin(section_header_offset, section_header_num_entries * sizeof(ElfSectionHeader), elf_size))
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
    uint64_t section_names_table_size = section_names_strtab->header.size;
    if (!rangeWithin(section_names_offset, section_names_table_size, elf_size))
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' is truncated (section names string table points after end of file)", path);

    section_names = mapped + section_names_offset;
    section_names_size = section_names_table_size;

    /// Require the trailing NUL (guaranteed by the ELF spec) so name() can return C strings without re-scanning.
    if (section_names_size == 0 || section_names[section_names_size - 1] != '\0')
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "The ELF '{}' has a section names string table that is not zero-terminated", path);

    /// Get program headers

    uint64_t program_header_offset = header->phoff;
    uint16_t program_header_num_entries = header->phnum;

    if (!program_header_offset
        || !program_header_num_entries
        || !rangeWithin(program_header_offset, program_header_num_entries * sizeof(ElfProgramHeader), elf_size))
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

        /// Skip sections that span past the end of file.
        if (!rangeWithin(section.header.offset, section.header.size, elf_size))
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


namespace
{

/// `avail_in` and `avail_out` are 32-bit, so the streams are fed in chunks.
bool inflateZlib(std::string_view source, char * destination, size_t size)
{
    constexpr size_t max_chunk = 1ULL << 30;
    z_stream stream{};
    if (inflateInit(&stream) != Z_OK)
        return false;

    stream.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(source.data()));
    stream.next_out = reinterpret_cast<Bytef *>(destination);
    size_t input_left = source.size();
    size_t output_left = size;
    int result = Z_OK;
    while (result == Z_OK)
    {
        if (stream.avail_in == 0)
        {
            stream.avail_in = static_cast<uInt>(std::min(input_left, max_chunk));
            input_left -= stream.avail_in;
        }
        if (stream.avail_out == 0)
        {
            stream.avail_out = static_cast<uInt>(std::min(output_left, max_chunk));
            output_left -= stream.avail_out;
        }
        result = inflate(&stream, Z_NO_FLUSH);
    }

    bool done = result == Z_STREAM_END && stream.avail_in == 0 && input_left == 0 && stream.avail_out == 0 && output_left == 0;
    inflateEnd(&stream);
    return done;
}

bool decompressSection(const ElfCompressionHeader & header, std::string_view payload, char * destination)
{
    if (header.type == ElfCompressionType::ZSTD)
    {
        size_t result = ZSTD_decompress(destination, header.size, payload.data(), payload.size());
        return !ZSTD_isError(result) && result == header.size;
    }
    return inflateZlib(payload, destination, header.size);
}

}


std::optional<std::vector<std::string_view>> Elf::getSectionsData(std::span<const char * const> names) const
{
    try
    {
        std::vector<std::string_view> result(names.size());
        std::vector<size_t> indices(names.size(), header->shnum);
        bool has_compressed = false;

        /// Not `findSectionByName`: it skips a section that runs past the end of the file, which must fail instead.
        for (size_t i = 0; i < names.size(); ++i)
        {
            for (size_t idx = 0; idx < header->shnum; ++idx)
            {
                Section section(section_headers[idx], *this);
                if (0 != strcmp(names[i], section.name()))
                    continue;
                if (section.header.type == SectionHeaderType::NOBITS)
                    break;
                if (!rangeWithin(section.header.offset, section.header.size, elf_size))
                    return std::nullopt;

                indices[i] = idx;
                if (section.header.flags & SectionHeaderFlag::COMPRESSED)
                    has_compressed = true;
                else
                    result[i] = {section.begin(), section.size()};
                break;
            }
        }

        if (!has_compressed)
            return result;

        std::lock_guard lock(decompression_mutex);

        std::vector<size_t> pending;
        for (size_t idx : indices)
        {
            if (idx == header->shnum || !(section_headers[idx].flags & SectionHeaderFlag::COMPRESSED))
                continue;
            if (unreadable_sections.contains(idx))
                return std::nullopt;
            if (!decompressed_sections.contains(idx) && std::find(pending.begin(), pending.end(), idx) == pending.end())
                pending.push_back(idx);
        }

        if (!pending.empty() && !decompressSections(pending))
        {
            unreadable_sections.insert(pending.begin(), pending.end());
            return std::nullopt;
        }

        for (size_t i = 0; i < names.size(); ++i)
        {
            if (auto it = decompressed_sections.find(indices[i]); it != decompressed_sections.end())
                result[i] = {it->second.data(), it->second.size()};
        }
        return result;
    }
    catch (const std::exception &) // Ok: an unreadable section only costs file:line in stack traces.
    {
        return std::nullopt;
    }
}


bool Elf::decompressSections(const std::vector<size_t> & indices) const
{
    std::vector<std::pair<ElfCompressionHeader, std::string_view>> inputs;
    uint64_t total_size = 0;
    for (size_t idx : indices)
    {
        Section section(section_headers[idx], *this);
        if (section.size() < sizeof(ElfCompressionHeader))
            return false;

        auto compression = unalignedLoad<ElfCompressionHeader>(section.begin());
        std::string_view payload(section.begin() + sizeof(ElfCompressionHeader), section.size() - sizeof(ElfCompressionHeader));

        /// Debug info compresses about 3-10 times; this bound rejects a corrupt size before allocation.
        constexpr size_t max_expansion_ratio = 1024;
        const size_t max_size = payload.size() > std::numeric_limits<size_t>::max() / max_expansion_ratio
            ? std::numeric_limits<size_t>::max()
            : payload.size() * max_expansion_ratio;

        if ((compression.type != ElfCompressionType::ZLIB && compression.type != ElfCompressionType::ZSTD)
            || compression.size > max_size
            || common::addOverflow(total_size, compression.size, total_size))
            return false;

        inputs.emplace_back(compression, payload);
    }

    /// Refused before anything is allocated if the sections do not fit the server limit.
    Int64 limit = total_memory_tracker.getHardLimit();
    Int64 used = std::max(total_memory_tracker.get(), total_memory_tracker.getRSS());
    if (limit > 0 && (used > limit || total_size > static_cast<uint64_t>(limit - used)))
        return false;

    bool done = false;
    try
    {
        std::vector<Memory<> *> sections;
        for (size_t idx : indices)
            sections.push_back(&decompressed_sections.try_emplace(idx).first->second);

        /// Kept for the life of this object, so a thread of its own allocates them: charged to the server and checked
        /// against its limit, not deferred by the thread (or charged to the query) that reads them first.
        std::thread([&]
        {
            std::vector<Memory<>> buffers;
            try
            {
                buffers.resize(indices.size());
                for (size_t i = 0; i < indices.size(); ++i)
                    if (inputs[i].first.size != 0)
                        buffers[i] = Memory<>(inputs[i].first.size);

                done = true;
                for (size_t i = 0; done && i < indices.size(); ++i)
                    if (inputs[i].first.size != 0)
                        done = decompressSection(inputs[i].first, inputs[i].second, buffers[i].data());
            }
            catch (const std::exception &) // Ok: reported as a failure below.
            {
                done = false;
            }

            if (done)
                for (size_t i = 0; i < indices.size(); ++i)
                    sections[i]->swap(buffers[i]);
        }).join();
    }
    catch (const std::exception &) // Ok: reported as a failure below.
    {
        done = false;
    }

    if (!done)
        for (size_t idx : indices)
            decompressed_sections.erase(idx);
    return done;
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
        {
            if (!rangeWithin(phdr.offset, phdr.filesz, elf_size))
                continue;
            return getBuildID(mapped + phdr.offset, phdr.filesz);
        }
    }

    return {};
}

String Elf::getBuildID(const char * nhdr_pos, size_t size)
{
    const char * nhdr_end = nhdr_pos + size;

    while (nhdr_pos < nhdr_end)
    {
        if (static_cast<size_t>(nhdr_end - nhdr_pos) < sizeof(ElfNameHeader))
            break;

        ElfNameHeader nhdr = unalignedLoad<ElfNameHeader>(nhdr_pos);
        nhdr_pos += sizeof(ElfNameHeader);

        if (nhdr.namesz > static_cast<size_t>(nhdr_end - nhdr_pos))
            break;
        nhdr_pos += nhdr.namesz;

        if (nhdr.descsz > static_cast<size_t>(nhdr_end - nhdr_pos))
            break;

        if (nhdr.type == NameHeaderType::GNU_BUILD_ID)
            return {nhdr_pos, nhdr.descsz};

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

    /// init() verified the table ends with a NUL, so a forward scan from any in-bounds offset stays in-bounds.
    if (header.name >= elf.section_names_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "Section name offset is out of bounds");

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
