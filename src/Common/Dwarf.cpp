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

#include <cstring>

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
#define DW_FORM_ref_sig8 0x20
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
#define DW_TAG_subprogram 0x2e
#define DW_TAG_try_block 0x32
#define DW_TAG_catch_block 0x25
#define DW_TAG_entry_point 0x03
#define DW_TAG_common_block 0x1a
#define DW_TAG_lexical_block 0x0b
#define DW_AT_stmt_list 0x10
#define DW_AT_comp_dir 0x1b
#define DW_AT_name 0x03
#define DW_AT_high_pc 0x12
#define DW_AT_low_pc 0x11
#define DW_AT_entry_pc 0x52
#define DW_AT_ranges 0x55
#define DW_AT_abstract_origin 0x31
#define DW_AT_call_line 0x59
#define DW_AT_call_file 0x58
#define DW_AT_linkage_name 0x6e
#define DW_AT_specification 0x47
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


Dwarf::Dwarf(const std::shared_ptr<Elf> & elf) : elf_(elf)
{
    init();
}

Dwarf::Section::Section(std::string_view d) : is64_bit(false), data(d)
{
}


#define SAFE_CHECK(cond, message) do { if (!(cond)) throw Exception(message, ErrorCodes::CANNOT_PARSE_DWARF); } while (false)


namespace
{
// Maximum number of DIEAbbreviation to cache in a compilation unit. Used to
// speed up inline function lookup.
const uint32_t kMaxAbbreviationEntries = 1000;

// All following read* functions read from a std::string_view, advancing the
// std::string_view, and aborting if there's not enough room.

// Read (bitwise) one object of type T
template <typename T>
requires std::is_trivial_v<T> && std::is_standard_layout_v<T>
T read(std::string_view & sp)
{
    SAFE_CHECK(sp.size() >= sizeof(T), fmt::format("underflow: expected bytes {}, got bytes {}", sizeof(T), sp.size()));
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
    SAFE_CHECK(len <= sp.size(), "invalid string length: " + std::to_string(len) + " vs. " + std::to_string(sp.size()));
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
    chunk = data;
    if (chunk.empty())
        return false;

    // Initial length is a uint32_t value for a 32-bit section, and
    // a 96-bit value (0xffffffff followed by the 64-bit length) for a 64-bit
    // section.
    auto initial_length = read<uint32_t>(chunk);
    is64_bit = (initial_length == uint32_t(-1));
    auto length = is64_bit ? read<uint64_t>(chunk) : initial_length;
    SAFE_CHECK(length <= chunk.size(), "invalid DWARF section");
    chunk = std::string_view(chunk.data(), length);
    data = std::string_view(chunk.end(), data.end() - chunk.end());
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
        elf_.reset();
        return;
    }

    // Optional: fast address range lookup. If missing .debug_info can
    // be used - but it's much slower (linear scan).
    getSection(".debug_aranges", &aranges_);

    getSection(".debug_ranges", &ranges_);
}

// static
bool Dwarf::readAbbreviation(std::string_view & section, DIEAbbreviation & abbr)
{
    // abbreviation code
    abbr.code = readULEB(section);
    if (abbr.code == 0)
        return false;

    // abbreviation tag
    abbr.tag = readULEB(section);

    // does this entry have children?
    abbr.has_children = (read<uint8_t>(section) != DW_CHILDREN_no);

    // attributes
    const char * attribute_begin = section.data();
    for (;;)
    {
        SAFE_CHECK(!section.empty(), "invalid attribute section");
        auto attr = readAttributeSpec(section);
        if (attr.name == 0 && attr.form == 0)
            break;
    }

    abbr.attributes = std::string_view(attribute_begin, section.data() - attribute_begin);
    return true;
}

// static
void Dwarf::readCompilationUnitAbbrs(std::string_view abbrev, CompilationUnit & cu)
{
    abbrev.remove_prefix(cu.abbrev_offset);

    DIEAbbreviation abbr;
    while (readAbbreviation(abbrev, abbr))
    {
        // Abbreviation code 0 is reserved for null debugging information entries.
        if (abbr.code != 0 && abbr.code <= kMaxAbbreviationEntries)
        {
            cu.abbr_cache[abbr.code - 1] = abbr;
        }
    }
}

size_t Dwarf::forEachChild(const CompilationUnit & cu, const Die & die, std::function<bool(const Die & die)> f) const
{
    size_t next_die_offset = forEachAttribute(cu, die, [&](const Attribute &) { return true; });
    if (!die.abbr.has_children)
    {
        return next_die_offset;
    }

    auto child_die = getDieAtOffset(cu, next_die_offset);
    while (child_die.code != 0)
    {
        if (!f(child_die))
        {
            return child_die.offset;
        }

        // NOTE: Don't run `f` over grandchildren, just skip over them.
        size_t sibling_offset = forEachChild(cu, child_die, [](const Die &) { return true; });
        child_die = getDieAtOffset(cu, sibling_offset);
    }

    // childDie is now a dummy die whose offset is to the code 0 marking the
    // end of the children. Need to add one to get the offset of the next die.
    return child_die.offset + 1;
}

/*
 * Iterate over all attributes of the given DIE, calling the given callable
 * for each. Iteration is stopped early if any of the calls return false.
 */
size_t Dwarf::forEachAttribute(const CompilationUnit & cu, const Die & die, std::function<bool(const Attribute & die)> f) const
{
    auto attrs = die.abbr.attributes;
    auto values = std::string_view{info_.data() + die.offset + die.attr_offset, cu.offset + cu.size - die.offset - die.attr_offset};
    while (auto spec = readAttributeSpec(attrs))
    {
        auto attr = readAttribute(die, spec, values);
        if (!f(attr))
        {
            return static_cast<size_t>(-1);
        }
    }
    return values.data() - info_.data();
}

Dwarf::Attribute Dwarf::readAttribute(const Die & die, AttributeSpec spec, std::string_view & info) const
{
    switch (spec.form)
    {
        case DW_FORM_addr:
            return {spec, die, read<uintptr_t>(info)};
        case DW_FORM_block1:
            return {spec, die, readBytes(info, read<uint8_t>(info))};
        case DW_FORM_block2:
            return {spec, die, readBytes(info, read<uint16_t>(info))};
        case DW_FORM_block4:
            return {spec, die, readBytes(info, read<uint32_t>(info))};
        case DW_FORM_block:
            [[fallthrough]];
        case DW_FORM_exprloc:
            return {spec, die, readBytes(info, readULEB(info))};
        case DW_FORM_data1:
            [[fallthrough]];
        case DW_FORM_ref1:
            return {spec, die, read<uint8_t>(info)};
        case DW_FORM_data2:
            [[fallthrough]];
        case DW_FORM_ref2:
            return {spec, die, read<uint16_t>(info)};
        case DW_FORM_data4:
            [[fallthrough]];
        case DW_FORM_ref4:
            return {spec, die, read<uint32_t>(info)};
        case DW_FORM_data8:
            [[fallthrough]];
        case DW_FORM_ref8:
            [[fallthrough]];
        case DW_FORM_ref_sig8:
            return {spec, die, read<uint64_t>(info)};
        case DW_FORM_sdata:
            return {spec, die, uint64_t(readSLEB(info))};
        case DW_FORM_udata:
            [[fallthrough]];
        case DW_FORM_ref_udata:
            return {spec, die, readULEB(info)};
        case DW_FORM_flag:
            return {spec, die, read<uint8_t>(info)};
        case DW_FORM_flag_present:
            return {spec, die, 1u};
        case DW_FORM_sec_offset:
            [[fallthrough]];
        case DW_FORM_ref_addr:
            return {spec, die, readOffset(info, die.is64Bit)};
        case DW_FORM_string:
            return {spec, die, readNullTerminated(info)};
        case DW_FORM_strp:
            return {spec, die, getStringFromStringSection(readOffset(info, die.is64Bit))};
        case DW_FORM_indirect: // form is explicitly specified
            // Update spec with the actual FORM.
            spec.form = readULEB(info);
            return readAttribute(die, spec, info);
        default:
            SAFE_CHECK(false, "invalid attribute form");
    }

    return {spec, die, 0u};
}

// static
Dwarf::AttributeSpec Dwarf::readAttributeSpec(std::string_view & sp)
{
    return {readULEB(sp), readULEB(sp)};
}

// static
Dwarf::CompilationUnit Dwarf::getCompilationUnit(std::string_view info, uint64_t offset)
{
    SAFE_CHECK(offset < info.size(), "unexpected offset");
    CompilationUnit cu;
    std::string_view chunk(info);
    cu.offset = offset;
    chunk.remove_prefix(offset);

    auto initial_length = read<uint32_t>(chunk);
    cu.is64Bit = (initial_length == uint32_t(-1));
    cu.size = cu.is64Bit ? read<uint64_t>(chunk) : initial_length;
    SAFE_CHECK(cu.size <= chunk.size(), "invalid chunk size");
    cu.size += cu.is64Bit ? 12 : 4;

    cu.version = read<uint16_t>(chunk);
    SAFE_CHECK(cu.version >= 2 && cu.version <= 4, "invalid info version");
    cu.abbrev_offset = readOffset(chunk, cu.is64Bit);
    cu.addr_size = read<uint8_t>(chunk);
    SAFE_CHECK(cu.addr_size == sizeof(uintptr_t), "invalid address size");

    cu.first_die = chunk.data() - info.data();
    return cu;
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

Dwarf::Die Dwarf::getDieAtOffset(const CompilationUnit & cu, uint64_t offset) const
{
    SAFE_CHECK(offset < info_.size(), fmt::format("unexpected offset {}, info size {}", offset, info_.size()));
    Die die;
    std::string_view sp{info_.data() + offset, cu.offset + cu.size - offset};
    die.offset = offset;
    die.is64Bit = cu.is64Bit;
    auto code = readULEB(sp);
    die.code = code;
    if (code == 0)
    {
        return die;
    }
    die.attr_offset = sp.data() - info_.data() - offset;
    die.abbr = !cu.abbr_cache.empty() && die.code < kMaxAbbreviationEntries ? cu.abbr_cache[die.code - 1]
                                                                            : getAbbreviation(die.code, cu.abbrev_offset);

    return die;
}

/**
 * Find the @locationInfo for @address in the compilation unit represented
 * by the @sp .debug_info entry.
 * Returns whether the address was found.
 * Advances @sp to the next entry in .debug_info.
 */
bool Dwarf::findLocation(
    uintptr_t address,
    const LocationInfoMode mode,
    CompilationUnit & cu,
    LocationInfo & info,
    std::vector<SymbolizedFrame> & inline_frames) const
{
    Die die = getDieAtOffset(cu, cu.first_die);
    // Partial compilation unit (DW_TAG_partial_unit) is not supported.
    SAFE_CHECK(die.abbr.tag == DW_TAG_compile_unit, "expecting compile unit entry");

    // Read attributes, extracting the few we care about
    std::optional<uint64_t> line_offset = 0;
    std::string_view compilation_directory;
    std::optional<std::string_view> main_file_name;
    std::optional<uint64_t> base_addr_cu;

    forEachAttribute(cu, die, [&](const Attribute & attr)
    {
        switch (attr.spec.name)
        {
            case DW_AT_stmt_list:
                // Offset in .debug_line for the line number VM program for this
                // compilation unit
                line_offset = std::get<uint64_t>(attr.attr_value);
                break;
            case DW_AT_comp_dir:
                // Compilation directory
                compilation_directory = std::get<std::string_view>(attr.attr_value);
                break;
            case DW_AT_name:
                // File name of main file being compiled
                main_file_name = std::get<std::string_view>(attr.attr_value);
                break;
            case DW_AT_low_pc:
            case DW_AT_entry_pc:
                // 2.17.1: historically DW_AT_low_pc was used. DW_AT_entry_pc was
                // introduced in DWARF3. Support either to determine the base address of
                // the CU.
                base_addr_cu = std::get<uint64_t>(attr.attr_value);
                break;
        }
        // Iterate through all attributes until find all above.
        return true;
    });

    if (main_file_name)
    {
        info.has_main_file = true;
        info.main_file = Path(compilation_directory, "", *main_file_name);
    }

    if (!line_offset)
    {
        return false;
    }

    std::string_view line_section(line_);
    line_section.remove_prefix(*line_offset);
    LineNumberVM line_vm(line_section, compilation_directory);

    // Execute line number VM program to find file and line
    info.has_file_and_line = line_vm.findAddress(address, info.file, info.line);

    bool check_inline = (mode == LocationInfoMode::FULL_WITH_INLINE);

    if (info.has_file_and_line && check_inline)
    {
        // Re-get the compilation unit with abbreviation cached.
        cu.abbr_cache.clear();
        cu.abbr_cache.resize(kMaxAbbreviationEntries);
        readCompilationUnitAbbrs(abbrev_, cu);

        // Find the subprogram that matches the given address.
        Die subprogram;
        findSubProgramDieForAddress(cu, die, address, base_addr_cu, subprogram);

        // Subprogram is the DIE of caller function.
        if (/*check_inline &&*/ subprogram.abbr.has_children)
        {
            // Use an extra location and get its call file and call line, so that
            // they can be used for the second last location when we don't have
            // enough inline frames for all inline functions call stack.
            const size_t max_size = Dwarf::kMaxInlineLocationInfoPerFrame + 1;
            std::vector<CallLocation> call_locations;
            call_locations.reserve(Dwarf::kMaxInlineLocationInfoPerFrame + 1);

            findInlinedSubroutineDieForAddress(cu, subprogram, line_vm, address, base_addr_cu, call_locations, max_size);
            size_t num_found = call_locations.size();

            if (num_found > 0)
            {
                const auto inner_most_file = info.file;
                const auto inner_most_line = info.line;

                // Earlier we filled in locationInfo:
                // - mainFile: the path to the CU -- the file where the non-inlined
                //   call is made from.
                // - file + line: the location of the inner-most inlined call.
                // Here we already find inlined info so mainFile would be redundant.
                info.has_main_file = false;
                info.main_file = Path{};
                // @findInlinedSubroutineDieForAddress fills inlineLocations[0] with the
                // file+line of the non-inlined outer function making the call.
                // locationInfo.name is already set by the caller by looking up the
                // non-inlined function @address belongs to.
                info.has_file_and_line = true; //-V1048
                info.file = call_locations[0].file;
                info.line = call_locations[0].line;

                // The next inlined subroutine's call file and call line is the current
                // caller's location.
                for (size_t i = 0; i < num_found - 1; ++i)
                {
                    call_locations[i].file = call_locations[i + 1].file;
                    call_locations[i].line = call_locations[i + 1].line;
                }
                // CallLocation for the inner-most inlined function:
                // - will be computed if enough space was available in the passed
                //   buffer.
                // - will have a .name, but no !.file && !.line
                // - its corresponding file+line is the one returned by LineVM based
                //   on @address.
                // Use the inner-most inlined file+line info we got from the LineVM.
                call_locations[num_found - 1].file = inner_most_file;
                call_locations[num_found - 1].line = inner_most_line;

                // Fill in inline frames in reverse order (as expected by the caller).
                std::reverse(call_locations.begin(), call_locations.end());
                for (const auto & call_location : call_locations)
                {
                    SymbolizedFrame inline_frame;
                    inline_frame.found = true;
                    inline_frame.addr = address;
                    if (!call_location.name.empty())
                        inline_frame.name = call_location.name.data();
                    else
                        inline_frame.name = nullptr;
                    inline_frame.location.has_file_and_line = true;
                    inline_frame.location.file = call_location.file;
                    inline_frame.location.line = call_location.line;
                    inline_frames.push_back(inline_frame);
                }
            }
        }
    }

    return info.has_file_and_line;
}

void Dwarf::findSubProgramDieForAddress(
    const CompilationUnit & cu, const Die & die, uint64_t address, std::optional<uint64_t> base_addr_cu, Die & subprogram) const
{
    forEachChild(cu, die, [&](const Die & child_die)
    {
        if (child_die.abbr.tag == DW_TAG_subprogram)
        {
            std::optional<uint64_t> low_pc;
            std::optional<uint64_t> high_pc;
            std::optional<bool> is_high_pc_addr;
            std::optional<uint64_t> range_offset;
            forEachAttribute(cu, child_die, [&](const Attribute & attr)
            {
                switch (attr.spec.name)
                {
                    case DW_AT_ranges:
                        range_offset = std::get<uint64_t>(attr.attr_value);
                        break;
                    case DW_AT_low_pc:
                        low_pc = std::get<uint64_t>(attr.attr_value);
                        break;
                    case DW_AT_high_pc:
                        // Value of DW_AT_high_pc attribute can be an address
                        // (DW_FORM_addr) or an offset (DW_FORM_data).
                        is_high_pc_addr = (attr.spec.form == DW_FORM_addr);
                        high_pc = std::get<uint64_t>(attr.attr_value);
                        break;
                }
                // Iterate through all attributes until find all above.
                return true;
            });
            bool pc_match = low_pc && high_pc && is_high_pc_addr && address >= *low_pc
                && (address < (*is_high_pc_addr ? *high_pc : *low_pc + *high_pc));
            bool range_match = range_offset && isAddrInRangeList(address, base_addr_cu, range_offset.value(), cu.addr_size);
            if (pc_match || range_match)
            {
                subprogram = child_die;
                return false;
            }
        }

        findSubProgramDieForAddress(cu, child_die, address, base_addr_cu, subprogram);

        // Iterates through children until find the inline subprogram.
        return true;
    });
}

/**
 * Find DW_TAG_inlined_subroutine child DIEs that contain @address and
 * then extract:
 * - Where was it called from (DW_AT_call_file & DW_AT_call_line):
 *   the statement or expression that caused the inline expansion.
 * - The inlined function's name. As a function may be inlined multiple
 *   times, common attributes like DW_AT_linkage_name or DW_AT_name
 *   are only stored in its "concrete out-of-line instance" (a
 *   DW_TAG_subprogram) which we find using DW_AT_abstract_origin.
 */
void Dwarf::findInlinedSubroutineDieForAddress(
    const CompilationUnit & cu,
    const Die & die,
    const LineNumberVM & line_vm,
    uint64_t address,
    std::optional<uint64_t> base_addr_cu,
    std::vector<CallLocation> & locations,
    const size_t max_size) const
{
    if (locations.size() >= max_size)
    {
        return;
    }

    forEachChild(cu, die, [&](const Die & child_die)
    {
        // Between a DW_TAG_subprogram and and DW_TAG_inlined_subroutine we might
        // have arbitrary intermediary "nodes", including DW_TAG_common_block,
        // DW_TAG_lexical_block, DW_TAG_try_block, DW_TAG_catch_block and
        // DW_TAG_with_stmt, etc.
        // We can't filter with locationhere since its range may be not specified.
        // See section 2.6.2: A location list containing only an end of list entry
        // describes an object that exists in the source code but not in the
        // executable program.
        if (child_die.abbr.tag == DW_TAG_try_block || child_die.abbr.tag == DW_TAG_catch_block || child_die.abbr.tag == DW_TAG_entry_point
            || child_die.abbr.tag == DW_TAG_common_block || child_die.abbr.tag == DW_TAG_lexical_block)
        {
            findInlinedSubroutineDieForAddress(cu, child_die, line_vm, address, base_addr_cu, locations, max_size);
            return true;
        }

        std::optional<uint64_t> low_pc;
        std::optional<uint64_t> high_pc;
        std::optional<bool> is_high_pc_addr;
        std::optional<uint64_t> abstract_origin;
        std::optional<uint64_t> abstract_origin_ref_type;
        std::optional<uint64_t> call_file;
        std::optional<uint64_t> call_line;
        std::optional<uint64_t> range_offset;
        forEachAttribute(cu, child_die, [&](const Attribute & attr)
        {
            switch (attr.spec.name)
            {
                case DW_AT_ranges:
                    range_offset = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_low_pc:
                    low_pc = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_high_pc:
                    // Value of DW_AT_high_pc attribute can be an address
                    // (DW_FORM_addr) or an offset (DW_FORM_data).
                    is_high_pc_addr = (attr.spec.form == DW_FORM_addr);
                    high_pc = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_abstract_origin:
                    abstract_origin_ref_type = attr.spec.form;
                    abstract_origin = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_call_line:
                    call_line = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_call_file:
                    call_file = std::get<uint64_t>(attr.attr_value);
                    break;
            }
            // Iterate through all until find all above attributes.
            return true;
        });

        // 2.17 Code Addresses and Ranges
        // Any debugging information entry describing an entity that has a
        // machine code address or range of machine code addresses,
        // which includes compilation units, module initialization, subroutines,
        // ordinary blocks, try/catch blocks, labels and the like, may have
        //  - A DW_AT_low_pc attribute for a single address,
        //  - A DW_AT_low_pc and DW_AT_high_pc pair of attributes for a
        //    single contiguous range of addresses, or
        //  - A DW_AT_ranges attribute for a non-contiguous range of addresses.
        // TODO: Support DW_TAG_entry_point and DW_TAG_common_block that don't
        // have DW_AT_low_pc/DW_AT_high_pc pairs and DW_AT_ranges.
        // TODO: Support relocated address which requires lookup in relocation map.
        bool pc_match
            = low_pc && high_pc && is_high_pc_addr && address >= *low_pc && (address < (*is_high_pc_addr ? *high_pc : *low_pc + *high_pc));
        bool range_match = range_offset && isAddrInRangeList(address, base_addr_cu, range_offset.value(), cu.addr_size);
        if (!pc_match && !range_match)
        {
            // Address doesn't match. Keep searching other children.
            return true;
        }

        if (!abstract_origin || !abstract_origin_ref_type || !call_line || !call_file)
        {
            // We expect a single sibling DIE to match on addr, but it's missing
            // required fields. Stop searching for other DIEs.
            return false;
        }

        CallLocation location;
        location.file = line_vm.getFullFileName(*call_file);
        location.line = *call_line;

        /// Something wrong with receiving debug info about inline.
        /// If set to true we stop parsing DWARF.
        bool die_for_inline_broken = false;

        auto get_function_name = [&](const CompilationUnit & srcu, uint64_t die_offset)
        {
            Die decl_die = getDieAtOffset(srcu, die_offset);
            auto & die_to_look_for_name = decl_die;

            Die def_die;
            // Jump to the actual function definition instead of declaration for name
            // and line info.
            // DW_AT_specification: Incomplete, non-defining, or separate declaration
            // corresponding to a declaration
            auto offset = getAttribute<uint64_t>(srcu, decl_die, DW_AT_specification);
            if (offset)
            {
                /// FIXME: actually it's a bug in our DWARF parser.
                ///
                /// Most of the times compilation unit offset (srcu.offset) is some big number inside .debug_info (like 434782255).
                /// Offset of DIE definition is some small relative number to srcu.offset (like 3518).
                /// However in some unknown cases offset looks like global, non relative number (like 434672579) and in this
                /// case we obviously doing something wrong parsing DWARF.
                ///
                /// What is important -- this bug? reproduces only with -flto=thin in release mode.
                /// Also llvm-dwarfdump --verify ./clickhouse says that our DWARF is ok, so it's another prove
                /// that we just doing something wrong.
                ///
                /// FIXME: Currently we just give up parsing DWARF for inlines when we got into this situation.
                if (srcu.offset + offset.value() >= info_.size())
                {
                    die_for_inline_broken = true;
                }
                else
                {
                    def_die = getDieAtOffset(srcu, srcu.offset + offset.value());
                    die_to_look_for_name = def_die;
                }
            }

            std::string_view name;

            if (die_for_inline_broken)
                return name;

            // The file and line will be set in the next inline subroutine based on
            // its DW_AT_call_file and DW_AT_call_line.
            forEachAttribute(srcu, die_to_look_for_name, [&](const Attribute & attr)
            {
                switch (attr.spec.name)
                {
                    case DW_AT_linkage_name:
                        name = std::get<std::string_view>(attr.attr_value);
                        break;
                    case DW_AT_name:
                        // NOTE: when DW_AT_linkage_name and DW_AT_name match, dwarf
                        // emitters omit DW_AT_linkage_name (to save space). If present
                        // DW_AT_linkage_name should always be preferred (mangled C++ name
                        // vs just the function name).
                        if (name.empty())
                        {
                            name = std::get<std::string_view>(attr.attr_value);
                        }
                        break;
                }
                return true;
            });
            return name;
        };

        // DW_AT_abstract_origin is a reference. There a 3 types of references:
        // - the reference can identify any debugging information entry within the
        //   compilation unit (DW_FORM_ref1, DW_FORM_ref2, DW_FORM_ref4,
        //   DW_FORM_ref8, DW_FORM_ref_udata). This type of reference is an offset
        //   from the first byte of the compilation header for the compilation unit
        //   containing the reference.
        // - the reference can identify any debugging information entry within a
        //   .debug_info section; in particular, it may refer to an entry in a
        //   different compilation unit (DW_FORM_ref_addr)
        // - the reference can identify any debugging information type entry that
        //   has been placed in its own type unit.
        //   Not applicable for DW_AT_abstract_origin.
        location.name = (*abstract_origin_ref_type != DW_FORM_ref_addr)
            ? get_function_name(cu, cu.offset + *abstract_origin)
            : get_function_name(findCompilationUnit(info_, *abstract_origin), *abstract_origin);

        /// FIXME: see comment above
        if (die_for_inline_broken)
            return false;

        locations.push_back(location);

        findInlinedSubroutineDieForAddress(cu, child_die, line_vm, address, base_addr_cu, locations, max_size);

        return false;
    });
}

bool Dwarf::findAddress(
    uintptr_t address, LocationInfo & locationInfo, LocationInfoMode mode, std::vector<SymbolizedFrame> & inline_frames) const
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
            auto unit = getCompilationUnit(info_, offset);
            findLocation(address, mode, unit, locationInfo, inline_frames);
            return locationInfo.has_file_and_line;
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
            SAFE_CHECK(mode == LocationInfoMode::FULL || mode == LocationInfoMode::FULL_WITH_INLINE, "unexpected mode");
            // Fall back to the linear scan.
        }
    }

    // Slow path (linear scan): Iterate over all .debug_info entries
    // and look for the address in each compilation unit.
    uint64_t offset = 0;
    while (offset < info_.size() && !locationInfo.has_file_and_line)
    {
        auto unit = getCompilationUnit(info_, offset);
        offset += unit.size;
        findLocation(address, mode, unit, locationInfo, inline_frames);
    }

    return locationInfo.has_file_and_line;
}

bool Dwarf::isAddrInRangeList(uint64_t address, std::optional<uint64_t> base_addr, size_t offset, uint8_t addr_size) const
{
    SAFE_CHECK(addr_size == 4 || addr_size == 8, "wrong address size");
    if (ranges_.empty())
    {
        return false;
    }

    const bool is_64bit_addr = addr_size == 8;
    std::string_view sp = ranges_;
    sp.remove_prefix(offset);
    const uint64_t max_addr = is_64bit_addr ? std::numeric_limits<uint64_t>::max() : std::numeric_limits<uint32_t>::max();
    while (!sp.empty())
    {
        uint64_t begin = readOffset(sp, is_64bit_addr);
        uint64_t end = readOffset(sp, is_64bit_addr);
        // The range list entry is a base address selection entry.
        if (begin == max_addr)
        {
            base_addr = end;
            continue;
        }
        // The range list entry is an end of list entry.
        if (begin == 0 && end == 0)
        {
            break;
        }
        // Check if the given address falls in the range list entry.
        // 2.17.3 Non-Contiguous Address Ranges
        // The applicable base address of a range list entry is determined by the
        // closest preceding base address selection entry (see below) in the same
        // range list. If there is no such selection entry, then the applicable base
        // address defaults to the base address of the compilation unit.
        if (base_addr && address >= begin + *base_addr && address < end + *base_addr)
        {
            return true;
        }
    }

    return false;
}

// static
Dwarf::CompilationUnit Dwarf::findCompilationUnit(std::string_view info, uint64_t targetOffset)
{
    SAFE_CHECK(targetOffset < info.size(), "unexpected target address");
    uint64_t offset = 0;
    while (offset < info.size())
    {
        std::string_view chunk(info);
        chunk.remove_prefix(offset);

        auto initial_length = read<uint32_t>(chunk);
        auto is_64bit = (initial_length == uint32_t(-1));
        auto size = is_64bit ? read<uint64_t>(chunk) : initial_length;
        SAFE_CHECK(size <= chunk.size(), "invalid chunk size");
        size += is_64bit ? 12 : 4;

        if (offset + size > targetOffset)
        {
            break;
        }
        offset += size;
    }
    return getCompilationUnit(info, offset);
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
