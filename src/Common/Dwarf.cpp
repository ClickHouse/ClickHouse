#if defined(__ELF__) && !defined(__FreeBSD__)

/*
 * Copyright 2012-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** This file was edited for ClickHouse.
  */

#include <optional>

#include <string.h>

#include <Common/Elf.h>
#include <Common/Dwarf.h>
#include <Common/Exception.h>


#define DW_CHILDREN_no 0
#define DW_FORM_addr 1
#define DW_FORM_block1 0x0a
#define DW_FORM_block2 3
#define DW_FORM_block4 4
#define DW_FORM_block 9
#define DW_FORM_exprloc 0x18
#define DW_FORM_data1 0x0b
#define DW_FORM_ref1 0x11
#define DW_FORM_data2 0x05
#define DW_FORM_ref2 0x12
#define DW_FORM_data4 0x06
#define DW_FORM_ref4 0x13
#define DW_FORM_data8 0x07
#define DW_FORM_ref8 0x14
#define DW_FORM_sdata 0x0d
#define DW_FORM_udata 0x0f
#define DW_FORM_ref_udata 0x15
#define DW_FORM_flag 0x0c
#define DW_FORM_flag_present 0x19
#define DW_FORM_sec_offset 0x17
#define DW_FORM_ref_addr 0x10
#define DW_FORM_string 0x08
#define DW_FORM_strp 0x0e
#define DW_FORM_indirect 0x16
#define DW_TAG_compile_unit 0x11
#define DW_AT_stmt_list 0x10
#define DW_AT_comp_dir 0x1b
#define DW_AT_name 0x03
#define DW_LNE_define_file 0x03
#define DW_LNS_copy 0x01
#define DW_LNS_advance_pc 0x02
#define DW_LNS_advance_line 0x03
#define DW_LNS_set_file 0x04
#define DW_LNS_set_column 0x05
#define DW_LNS_negate_stmt 0x06
#define DW_LNS_set_basic_block 0x07
#define DW_LNS_const_add_pc 0x08
#define DW_LNS_fixed_advance_pc 0x09
#define DW_LNS_set_prologue_end 0x0a
#define DW_LNS_set_epilogue_begin 0x0b
#define DW_LNS_set_isa 0x0c
#define DW_LNE_end_sequence 0x01
#define DW_LNE_set_address 0x02
#define DW_LNE_set_discriminator 0x04


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DWARF;
}


Dwarf::Dwarf(const Elf & elf) : elf_(&elf)
{
    init();
}

Dwarf::Section::Section(std::string_view d) : is64Bit_(false), data_(d)
{
}


#define SAFE_CHECK(cond, message) do { if (!(cond)) throw Exception(message, ErrorCodes::CANNOT_PARSE_DWARF); } while (false)


namespace
{
// All following read* functions read from a std::string_view, advancing the
// std::string_view, and aborting if there's not enough room.

// Read (bitwise) one object of type T
template <typename T>
std::enable_if_t<std::is_trivial_v<T> && std::is_standard_layout_v<T>, T> read(std::string_view & sp)
{
    SAFE_CHECK(sp.size() >= sizeof(T), "underflow");
    T x;
    memcpy(&x, sp.data(), sizeof(T));
    sp.remove_prefix(sizeof(T));
    return x;
}

// Read ULEB (unsigned) varint value; algorithm from the DWARF spec
uint64_t readULEB(std::string_view & sp, uint8_t & shift, uint8_t & val)
{
    uint64_t r = 0;
    shift = 0;
    do
    {
        val = read<uint8_t>(sp);
        r |= (uint64_t(val & 0x7f) << shift);
        shift += 7;
    } while (val & 0x80);
    return r;
}

uint64_t readULEB(std::string_view & sp)
{
    uint8_t shift;
    uint8_t val;
    return readULEB(sp, shift, val);
}

// Read SLEB (signed) varint value; algorithm from the DWARF spec
int64_t readSLEB(std::string_view & sp)
{
    uint8_t shift;
    uint8_t val;
    uint64_t r = readULEB(sp, shift, val);

    if (shift < 64 && (val & 0x40))
    {
        r |= -(1ULL << shift); // sign extend
    }

    return r;
}

// Read a value of "section offset" type, which may be 4 or 8 bytes
uint64_t readOffset(std::string_view & sp, bool is64Bit)
{
    return is64Bit ? read<uint64_t>(sp) : read<uint32_t>(sp);
}

// Read "len" bytes
std::string_view readBytes(std::string_view & sp, uint64_t len)
{
    SAFE_CHECK(len >= sp.size(), "invalid string length");
    std::string_view ret(sp.data(), len);
    sp.remove_prefix(len);
    return ret;
}

// Read a null-terminated string
std::string_view readNullTerminated(std::string_view & sp)
{
    const char * p = static_cast<const char *>(memchr(sp.data(), 0, sp.size()));
    SAFE_CHECK(p, "invalid null-terminated string");
    std::string_view ret(sp.data(), p - sp.data());
    sp = std::string_view(p + 1, sp.size());
    return ret;
}

// Skip over padding until sp.data() - start is a multiple of alignment
void skipPadding(std::string_view & sp, const char * start, size_t alignment)
{
    size_t remainder = (sp.data() - start) % alignment;
    if (remainder)
    {
        SAFE_CHECK(alignment - remainder <= sp.size(), "invalid padding");
        sp.remove_prefix(alignment - remainder);
    }
}

}


Dwarf::Path::Path(std::string_view baseDir, std::string_view subDir, std::string_view file)
    : baseDir_(baseDir), subDir_(subDir), file_(file)
{
    using std::swap;

    // Normalize
    if (file_.empty())
    {
        baseDir_ = {};
        subDir_ = {};
        return;
    }

    if (file_[0] == '/')
    {
        // file_ is absolute
        baseDir_ = {};
        subDir_ = {};
    }

    if (!subDir_.empty() && subDir_[0] == '/')
    {
        baseDir_ = {}; // subDir_ is absolute
    }

    // Make sure it's never the case that baseDir_ is empty, but subDir_ isn't.
    if (baseDir_.empty())
    {
        swap(baseDir_, subDir_);
    }
}

size_t Dwarf::Path::size() const
{
    size_t size = 0;
    bool needs_slash = false;

    if (!baseDir_.empty())
    {
        size += baseDir_.size();
        needs_slash = baseDir_.back() != '/';
    }

    if (!subDir_.empty())
    {
        size += needs_slash;
        size += subDir_.size();
        needs_slash = subDir_.back() != '/';
    }

    if (!file_.empty())
    {
        size += needs_slash;
        size += file_.size();
    }

    return size;
}

size_t Dwarf::Path::toBuffer(char * buf, size_t bufSize) const
{
    size_t total_size = 0;
    bool needs_slash = false;

    auto append = [&](std::string_view sp)
    {
        if (bufSize >= 2)
        {
            size_t to_copy = std::min(sp.size(), bufSize - 1);
            memcpy(buf, sp.data(), to_copy);
            buf += to_copy;
            bufSize -= to_copy;
        }
        total_size += sp.size();
    };

    if (!baseDir_.empty())
    {
        append(baseDir_);
        needs_slash = baseDir_.back() != '/';
    }
    if (!subDir_.empty())
    {
        if (needs_slash)
        {
            append("/");
        }
        append(subDir_);
        needs_slash = subDir_.back() != '/';
    }
    if (!file_.empty())
    {
        if (needs_slash)
        {
            append("/");
        }
        append(file_);
    }
    if (bufSize)
    {
        *buf = '\0';
    }

    SAFE_CHECK(total_size == size(), "Size mismatch");
    return total_size;
}

void Dwarf::Path::toString(std::string & dest) const
{
    size_t initial_size = dest.size();
    dest.reserve(initial_size + size());
    if (!baseDir_.empty())
    {
        dest.append(baseDir_.begin(), baseDir_.end());
    }
    if (!subDir_.empty())
    {
        if (!dest.empty() && dest.back() != '/')
        {
            dest.push_back('/');
        }
        dest.append(subDir_.begin(), subDir_.end());
    }
    if (!file_.empty())
    {
        if (!dest.empty() && dest.back() != '/')
        {
            dest.push_back('/');
        }
        dest.append(file_.begin(), file_.end());
    }
    SAFE_CHECK(dest.size() == initial_size + size(), "Size mismatch");
}

// Next chunk in section
bool Dwarf::Section::next(std::string_view & chunk)
{
    chunk = data_;
    if (chunk.empty())
        return false;

    // Initial length is a uint32_t value for a 32-bit section, and
    // a 96-bit value (0xffffffff followed by the 64-bit length) for a 64-bit
    // section.
    auto initial_length = read<uint32_t>(chunk);
    is64Bit_ = (initial_length == uint32_t(-1));
    auto length = is64Bit_ ? read<uint64_t>(chunk) : initial_length;
    SAFE_CHECK(length <= chunk.size(), "invalid DWARF section");
    chunk = std::string_view(chunk.data(), length);
    data_ = std::string_view(chunk.end(), data_.end() - chunk.end());
    return true;
}

bool Dwarf::getSection(const char * name, std::string_view * section) const
{
    std::optional<Elf::Section> elf_section = elf_->findSectionByName(name);
    if (!elf_section)
        return false;

#ifdef SHF_COMPRESSED
    if (elf_section->header.sh_flags & SHF_COMPRESSED)
        return false;
#endif

    *section = { elf_section->begin(), elf_section->size()};
    return true;
}

void Dwarf::init()
{
    // Make sure that all .debug_* sections exist
    if (!getSection(".debug_info", &info_)
        || !getSection(".debug_abbrev", &abbrev_)
        || !getSection(".debug_line", &line_)
        || !getSection(".debug_str", &strings_))
    {
        elf_ = nullptr;
        return;
    }

    // Optional: fast address range lookup. If missing .debug_info can
    // be used - but it's much slower (linear scan).
    getSection(".debug_aranges", &aranges_);
}

bool Dwarf::readAbbreviation(std::string_view & section, DIEAbbreviation & abbr)
{
    // abbreviation code
    abbr.code = readULEB(section);
    if (abbr.code == 0)
        return false;

    // abbreviation tag
    abbr.tag = readULEB(section);

    // does this entry have children?
    abbr.hasChildren = (read<uint8_t>(section) != DW_CHILDREN_no);

    // attributes
    const char * attribute_begin = section.data();
    for (;;)
    {
        SAFE_CHECK(!section.empty(), "invalid attribute section");
        auto attr = readAttribute(section);
        if (attr.name == 0 && attr.form == 0)
            break;
    }

    abbr.attributes = std::string_view(attribute_begin, section.data() - attribute_begin);
    return true;
}

Dwarf::DIEAbbreviation::Attribute Dwarf::readAttribute(std::string_view & sp)
{
    return {readULEB(sp), readULEB(sp)};
}

Dwarf::DIEAbbreviation Dwarf::getAbbreviation(uint64_t code, uint64_t offset) const
{
    // Linear search in the .debug_abbrev section, starting at offset
    std::string_view section = abbrev_;
    section.remove_prefix(offset);

    Dwarf::DIEAbbreviation abbr;
    while (readAbbreviation(section, abbr))
        if (abbr.code == code)
            return abbr;

    SAFE_CHECK(false, "could not find abbreviation code");
}

Dwarf::AttributeValue Dwarf::readAttributeValue(std::string_view & sp, uint64_t form, bool is64Bit) const
{
    switch (form)
    {
        case DW_FORM_addr:
            return uint64_t(read<uintptr_t>(sp));
        case DW_FORM_block1:
            return readBytes(sp, read<uint8_t>(sp));
        case DW_FORM_block2:
            return readBytes(sp, read<uint16_t>(sp));
        case DW_FORM_block4:
            return readBytes(sp, read<uint32_t>(sp));
        case DW_FORM_block: [[fallthrough]];
        case DW_FORM_exprloc:
            return readBytes(sp, readULEB(sp));
        case DW_FORM_data1: [[fallthrough]];
        case DW_FORM_ref1:
            return uint64_t(read<uint8_t>(sp));
        case DW_FORM_data2: [[fallthrough]];
        case DW_FORM_ref2:
            return uint64_t(read<uint16_t>(sp));
        case DW_FORM_data4: [[fallthrough]];
        case DW_FORM_ref4:
            return uint64_t(read<uint32_t>(sp));
        case DW_FORM_data8: [[fallthrough]];
        case DW_FORM_ref8:
            return read<uint64_t>(sp);
        case DW_FORM_sdata:
            return uint64_t(readSLEB(sp));
        case DW_FORM_udata: [[fallthrough]];
        case DW_FORM_ref_udata:
            return readULEB(sp);
        case DW_FORM_flag:
            return uint64_t(read<uint8_t>(sp));
        case DW_FORM_flag_present:
            return uint64_t(1);
        case DW_FORM_sec_offset: [[fallthrough]];
        case DW_FORM_ref_addr:
            return readOffset(sp, is64Bit);
        case DW_FORM_string:
            return readNullTerminated(sp);
        case DW_FORM_strp:
            return getStringFromStringSection(readOffset(sp, is64Bit));
        case DW_FORM_indirect: // form is explicitly specified
            return readAttributeValue(sp, readULEB(sp), is64Bit);
        default:
            SAFE_CHECK(false, "invalid attribute form");
    }
}

std::string_view Dwarf::getStringFromStringSection(uint64_t offset) const
{
    SAFE_CHECK(offset < strings_.size(), "invalid strp offset");
    std::string_view sp(strings_);
    sp.remove_prefix(offset);
    return readNullTerminated(sp);
}

/**
 * Find @address in .debug_aranges and return the offset in
 * .debug_info for compilation unit to which this address belongs.
 */
bool Dwarf::findDebugInfoOffset(uintptr_t address, std::string_view aranges, uint64_t & offset)
{
    Section aranges_section(aranges);
    std::string_view chunk;
    while (aranges_section.next(chunk))
    {
        auto version = read<uint16_t>(chunk);
        SAFE_CHECK(version == 2, "invalid aranges version");

        offset = readOffset(chunk, aranges_section.is64Bit());
        auto address_size = read<uint8_t>(chunk);
        SAFE_CHECK(address_size == sizeof(uintptr_t), "invalid address size");
        auto segment_size = read<uint8_t>(chunk);
        SAFE_CHECK(segment_size == 0, "segmented architecture not supported");

        // Padded to a multiple of 2 addresses.
        // Strangely enough, this is the only place in the DWARF spec that requires
        // padding.
        skipPadding(chunk, aranges.data(), 2 * sizeof(uintptr_t));
        for (;;)
        {
            auto start = read<uintptr_t>(chunk);
            auto length = read<uintptr_t>(chunk);

            if (start == 0 && length == 0)
                break;

            // Is our address in this range?
            if (address >= start && address < start + length)
                return true;
        }
    }
    return false;
}

/**
 * Find the @locationInfo for @address in the compilation unit represented
 * by the @sp .debug_info entry.
 * Returns whether the address was found.
 * Advances @sp to the next entry in .debug_info.
 */
bool Dwarf::findLocation(uintptr_t address, std::string_view & infoEntry, LocationInfo & locationInfo) const
{
    // For each compilation unit compiled with a DWARF producer, a
    // contribution is made to the .debug_info section of the object
    // file. Each such contribution consists of a compilation unit
    // header (see Section 7.5.1.1) followed by a single
    // DW_TAG_compile_unit or DW_TAG_partial_unit debugging information
    // entry, together with its children.

    // 7.5.1.1 Compilation Unit Header
    //  1. unit_length (4B or 12B): read by Section::next
    //  2. version (2B)
    //  3. debug_abbrev_offset (4B or 8B): offset into the .debug_abbrev section
    //  4. address_size (1B)

    Section debug_info_section(infoEntry);
    std::string_view chunk;
    SAFE_CHECK(debug_info_section.next(chunk), "invalid debug info");

    auto version = read<uint16_t>(chunk);
    SAFE_CHECK(version >= 2 && version <= 4, "invalid info version");
    uint64_t abbrev_offset = readOffset(chunk, debug_info_section.is64Bit());
    auto address_size = read<uint8_t>(chunk);
    SAFE_CHECK(address_size == sizeof(uintptr_t), "invalid address size");

    // We survived so far. The first (and only) DIE should be DW_TAG_compile_unit
    // NOTE: - binutils <= 2.25 does not issue DW_TAG_partial_unit.
    //       - dwarf compression tools like `dwz` may generate it.
    // TODO(tudorb): Handle DW_TAG_partial_unit?
    auto code = readULEB(chunk);
    SAFE_CHECK(code != 0, "invalid code");
    auto abbr = getAbbreviation(code, abbrev_offset);
    SAFE_CHECK(abbr.tag == DW_TAG_compile_unit, "expecting compile unit entry");
    // Skip children entries, remove_prefix to the next compilation unit entry.
    infoEntry.remove_prefix(chunk.end() - infoEntry.begin());

    // Read attributes, extracting the few we care about
    bool found_line_offset = false;
    uint64_t line_offset = 0;
    std::string_view compilation_directory;
    std::string_view main_file_name;

    DIEAbbreviation::Attribute attr;
    std::string_view attributes = abbr.attributes;
    for (;;)
    {
        attr = readAttribute(attributes);
        if (attr.name == 0 && attr.form == 0)
        {
            break;
        }
        auto val = readAttributeValue(chunk, attr.form, debug_info_section.is64Bit());
        switch (attr.name)
        {
            case DW_AT_stmt_list:
                // Offset in .debug_line for the line number VM program for this
                // compilation unit
                line_offset = std::get<uint64_t>(val);
                found_line_offset = true;
                break;
            case DW_AT_comp_dir:
                // Compilation directory
                compilation_directory = std::get<std::string_view>(val);
                break;
            case DW_AT_name:
                // File name of main file being compiled
                main_file_name = std::get<std::string_view>(val);
                break;
        }
    }

    if (!main_file_name.empty())
    {
        locationInfo.hasMainFile = true;
        locationInfo.mainFile = Path(compilation_directory, "", main_file_name);
    }

    if (!found_line_offset)
    {
        return false;
    }

    std::string_view line_section(line_);
    line_section.remove_prefix(line_offset);
    LineNumberVM line_vm(line_section, compilation_directory);

    // Execute line number VM program to find file and line
    locationInfo.hasFileAndLine = line_vm.findAddress(address, locationInfo.file, locationInfo.line);
    return locationInfo.hasFileAndLine;
}

bool Dwarf::findAddress(uintptr_t address, LocationInfo & locationInfo, LocationInfoMode mode) const
{
    locationInfo = LocationInfo();

    if (mode == LocationInfoMode::DISABLED)
    {
        return false;
    }

    if (!elf_)
    { // No file.
        return false;
    }

    if (!aranges_.empty())
    {
        // Fast path: find the right .debug_info entry by looking up the
        // address in .debug_aranges.
        uint64_t offset = 0;
        if (findDebugInfoOffset(address, aranges_, offset))
        {
            // Read compilation unit header from .debug_info
            std::string_view info_entry(info_);
            info_entry.remove_prefix(offset);
            findLocation(address, info_entry, locationInfo);
            return locationInfo.hasFileAndLine;
        }
        else if (mode == LocationInfoMode::FAST)
        {
            // NOTE: Clang (when using -gdwarf-aranges) doesn't generate entries
            // in .debug_aranges for some functions, but always generates
            // .debug_info entries.  Scanning .debug_info is slow, so fall back to
            // it only if such behavior is requested via LocationInfoMode.
            return false;
        }
        else
        {
            SAFE_CHECK(mode == LocationInfoMode::FULL, "unexpected mode");
            // Fall back to the linear scan.
        }
    }

    // Slow path (linear scan): Iterate over all .debug_info entries
    // and look for the address in each compilation unit.
    std::string_view info_entry(info_);
    while (!info_entry.empty() && !locationInfo.hasFileAndLine)
        findLocation(address, info_entry, locationInfo);

    return locationInfo.hasFileAndLine;
}

Dwarf::LineNumberVM::LineNumberVM(std::string_view data, std::string_view compilationDirectory)
    : compilationDirectory_(compilationDirectory)
{
    Section section(data);
    SAFE_CHECK(section.next(data_), "invalid line number VM");
    is64Bit_ = section.is64Bit();
    init();
    reset();
}

void Dwarf::LineNumberVM::reset()
{
    address_ = 0;
    file_ = 1;
    line_ = 1;
    column_ = 0;
    isStmt_ = defaultIsStmt_;
    basicBlock_ = false;
    endSequence_ = false;
    prologueEnd_ = false;
    epilogueBegin_ = false;
    isa_ = 0;
    discriminator_ = 0;
}

void Dwarf::LineNumberVM::init()
{
    version_ = read<uint16_t>(data_);
    SAFE_CHECK(version_ >= 2 && version_ <= 4, "invalid version in line number VM");
    uint64_t header_length = readOffset(data_, is64Bit_);
    SAFE_CHECK(header_length <= data_.size(), "invalid line number VM header length");
    std::string_view header(data_.data(), header_length);
    data_ = std::string_view(header.end(), data_.end() - header.end());

    minLength_ = read<uint8_t>(header);
    if (version_ == 4)
    { // Version 2 and 3 records don't have this
        uint8_t max_ops_per_instruction = read<uint8_t>(header);
        SAFE_CHECK(max_ops_per_instruction == 1, "VLIW not supported");
    }
    defaultIsStmt_ = read<uint8_t>(header);
    lineBase_ = read<int8_t>(header); // yes, signed
    lineRange_ = read<uint8_t>(header);
    opcodeBase_ = read<uint8_t>(header);
    SAFE_CHECK(opcodeBase_ != 0, "invalid opcode base");
    standardOpcodeLengths_ = reinterpret_cast<const uint8_t *>(header.data()); //-V506
    header.remove_prefix(opcodeBase_ - 1);

    // We don't want to use heap, so we don't keep an unbounded amount of state.
    // We'll just skip over include directories and file names here, and
    // we'll loop again when we actually need to retrieve one.
    std::string_view sp;
    const char * tmp = header.data();
    includeDirectoryCount_ = 0;
    while (!(sp = readNullTerminated(header)).empty())
    {
        ++includeDirectoryCount_;
    }
    includeDirectories_ = std::string_view(tmp, header.data() - tmp);

    tmp = header.data();
    FileName fn;
    fileNameCount_ = 0;
    while (readFileName(header, fn))
    {
        ++fileNameCount_;
    }
    fileNames_ = std::string_view(tmp, header.data() - tmp);
}

bool Dwarf::LineNumberVM::next(std::string_view & program)
{
    Dwarf::LineNumberVM::StepResult ret;
    do
    {
        ret = step(program);
    } while (ret == CONTINUE);

    return (ret == COMMIT);
}

Dwarf::LineNumberVM::FileName Dwarf::LineNumberVM::getFileName(uint64_t index) const
{
    SAFE_CHECK(index != 0, "invalid file index 0");

    FileName fn;
    if (index <= fileNameCount_)
    {
        std::string_view file_names = fileNames_;
        for (; index; --index)
        {
            if (!readFileName(file_names, fn))
            {
                abort();
            }
        }
        return fn;
    }

    index -= fileNameCount_;

    std::string_view program = data_;
    for (; index; --index)
    {
        SAFE_CHECK(nextDefineFile(program, fn), "invalid file index");
    }

    return fn;
}

std::string_view Dwarf::LineNumberVM::getIncludeDirectory(uint64_t index) const
{
    if (index == 0)
    {
        return std::string_view();
    }

    SAFE_CHECK(index <= includeDirectoryCount_, "invalid include directory");

    std::string_view include_directories = includeDirectories_;
    std::string_view dir;
    for (; index; --index)
    {
        dir = readNullTerminated(include_directories);
        if (dir.empty())
        {
            abort(); // BUG
        }
    }

    return dir;
}

bool Dwarf::LineNumberVM::readFileName(std::string_view & program, FileName & fn)
{
    fn.relativeName = readNullTerminated(program);
    if (fn.relativeName.empty())
    {
        return false;
    }
    fn.directoryIndex = readULEB(program);
    // Skip over file size and last modified time
    readULEB(program);
    readULEB(program);
    return true;
}

bool Dwarf::LineNumberVM::nextDefineFile(std::string_view & program, FileName & fn) const
{
    while (!program.empty())
    {
        auto opcode = read<uint8_t>(program);

        if (opcode >= opcodeBase_)
        { // special opcode
            continue;
        }

        if (opcode != 0)
        { // standard opcode
            // Skip, slurp the appropriate number of LEB arguments
            uint8_t arg_count = standardOpcodeLengths_[opcode - 1];
            while (arg_count--)
            {
                readULEB(program);
            }
            continue;
        }

        // Extended opcode
        auto length = readULEB(program);
        // the opcode itself should be included in the length, so length >= 1
        SAFE_CHECK(length != 0, "invalid extended opcode length");
        read<uint8_t>(program); // extended opcode
        --length;

        if (opcode == DW_LNE_define_file)
        {
            SAFE_CHECK(readFileName(program, fn), "invalid empty file in DW_LNE_define_file");
            return true;
        }

        program.remove_prefix(length);
    }

    return false;
}

Dwarf::LineNumberVM::StepResult Dwarf::LineNumberVM::step(std::string_view & program)
{
    auto opcode = read<uint8_t>(program);

    if (opcode >= opcodeBase_)
    { // special opcode
        uint8_t adjusted_opcode = opcode - opcodeBase_;
        uint8_t op_advance = adjusted_opcode / lineRange_;

        address_ += minLength_ * op_advance;
        line_ += lineBase_ + adjusted_opcode % lineRange_;

        basicBlock_ = false;
        prologueEnd_ = false;
        epilogueBegin_ = false;
        discriminator_ = 0;
        return COMMIT;
    }

    if (opcode != 0)
    { // standard opcode
        // Only interpret opcodes that are recognized by the version we're parsing;
        // the others are vendor extensions and we should ignore them.
        switch (opcode)
        {
            case DW_LNS_copy:
                basicBlock_ = false;
                prologueEnd_ = false;
                epilogueBegin_ = false;
                discriminator_ = 0;
                return COMMIT;
            case DW_LNS_advance_pc:
                address_ += minLength_ * readULEB(program);
                return CONTINUE;
            case DW_LNS_advance_line:
                line_ += readSLEB(program);
                return CONTINUE;
            case DW_LNS_set_file:
                file_ = readULEB(program);
                return CONTINUE;
            case DW_LNS_set_column:
                column_ = readULEB(program);
                return CONTINUE;
            case DW_LNS_negate_stmt:
                isStmt_ = !isStmt_;
                return CONTINUE;
            case DW_LNS_set_basic_block:
                basicBlock_ = true;
                return CONTINUE;
            case DW_LNS_const_add_pc:
                address_ += minLength_ * ((255 - opcodeBase_) / lineRange_);
                return CONTINUE;
            case DW_LNS_fixed_advance_pc:
                address_ += read<uint16_t>(program);
                return CONTINUE;
            case DW_LNS_set_prologue_end:
                if (version_ == 2)
                {
                    break; // not supported in version 2
                }
                prologueEnd_ = true;
                return CONTINUE;
            case DW_LNS_set_epilogue_begin:
                if (version_ == 2)
                {
                    break; // not supported in version 2
                }
                epilogueBegin_ = true;
                return CONTINUE;
            case DW_LNS_set_isa:
                if (version_ == 2)
                {
                    break; // not supported in version 2
                }
                isa_ = readULEB(program);
                return CONTINUE;
        }

        // Unrecognized standard opcode, slurp the appropriate number of LEB
        // arguments.
        uint8_t arg_count = standardOpcodeLengths_[opcode - 1];
        while (arg_count--)
        {
            readULEB(program);
        }
        return CONTINUE;
    }

    // Extended opcode
    auto length = readULEB(program);
    // the opcode itself should be included in the length, so length >= 1
    SAFE_CHECK(length != 0, "invalid extended opcode length");
    auto extended_opcode = read<uint8_t>(program);
    --length;

    switch (extended_opcode)
    {
        case DW_LNE_end_sequence:
            return END;
        case DW_LNE_set_address:
            address_ = read<uintptr_t>(program);
            return CONTINUE;
        case DW_LNE_define_file:
            // We can't process DW_LNE_define_file here, as it would require us to
            // use unbounded amounts of state (ie. use the heap).  We'll do a second
            // pass (using nextDefineFile()) if necessary.
            break;
        case DW_LNE_set_discriminator:
            discriminator_ = readULEB(program);
            return CONTINUE;
    }

    // Unrecognized extended opcode
    program.remove_prefix(length);
    return CONTINUE;
}

bool Dwarf::LineNumberVM::findAddress(uintptr_t target, Path & file, uint64_t & line)
{
    std::string_view program = data_;

    // Within each sequence of instructions, the address may only increase.
    // Unfortunately, within the same compilation unit, sequences may appear
    // in any order.  So any sequence is a candidate if it starts at an address
    // <= the target address, and we know we've found the target address if
    // a candidate crosses the target address.
    enum State
    {
        START,
        LOW_SEQ, // candidate
        HIGH_SEQ
    };
    State state = START;
    reset();

    uint64_t prev_file = 0;
    uint64_t prev_line = 0;
    while (!program.empty())
    {
        bool seq_end = !next(program);

        if (state == START)
        {
            if (!seq_end)
            {
                state = address_ <= target ? LOW_SEQ : HIGH_SEQ;
            }
        }

        if (state == LOW_SEQ)
        {
            if (address_ > target)
            {
                // Found it!  Note that ">" is indeed correct (not ">="), as each
                // sequence is guaranteed to have one entry past-the-end (emitted by
                // DW_LNE_end_sequence)
                if (prev_file == 0)
                {
                    return false;
                }
                auto fn = getFileName(prev_file);
                file = Path(compilationDirectory_, getIncludeDirectory(fn.directoryIndex), fn.relativeName);
                line = prev_line;
                return true;
            }
            prev_file = file_;
            prev_line = line_;
        }

        if (seq_end)
        {
            state = START;
            reset();
        }
    }

    return false;
}

}

#endif
