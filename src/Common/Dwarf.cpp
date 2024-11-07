#if defined(__ELF__) && !defined(OS_FREEBSD)

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
#define DW_FORM_strx 0x1a
#define DW_FORM_addrx 0x1b
#define DW_FORM_ref_sup4 0x1c
#define DW_FORM_strp_sup 0x1d
#define DW_FORM_data16 0x1e
#define DW_FORM_line_strp 0x1f
#define DW_FORM_implicit_const 0x21
#define DW_FORM_rnglistx 0x23
#define DW_FORM_loclistx 0x22
#define DW_FORM_ref_sup8 0x24
#define DW_FORM_strx1 0x25
#define DW_FORM_strx2 0x26
#define DW_FORM_strx3 0x27
#define DW_FORM_strx4 0x28
#define DW_FORM_addrx1 0x29
#define DW_FORM_addrx2 0x2a
#define DW_FORM_addrx3 0x2b
#define DW_FORM_addrx4 0x2c

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
#define DW_AT_str_offsets_base 0x72
#define DW_AT_addr_base 0x73
#define DW_AT_rnglists_base 0x74
#define DW_AT_loclists_base 0x8c
#define DW_AT_GNU_ranges_base 0x2132
#define DW_AT_GNU_addr_base 0x2133

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

#define DW_LNCT_path 0x1
#define DW_LNCT_directory_index 0x2
#define DW_LNCT_timestamp 0x3
#define DW_LNCT_size 0x4
#define DW_LNCT_MD5 0x5

#define DW_RLE_end_of_list 0x0
#define DW_RLE_base_addressx 0x1
#define DW_RLE_startx_endx 0x2
#define DW_RLE_startx_length 0x3
#define DW_RLE_offset_pair 0x4
#define DW_RLE_base_address 0x5
#define DW_RLE_start_end 0x6
#define DW_RLE_start_length 0x7


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DWARF;
}


Dwarf::Dwarf(const std::shared_ptr<Elf> & elf)
    : elf_(elf)
    , abbrev_(getSection(".debug_abbrev"))
    , addr_(getSection(".debug_addr"))
    , aranges_(getSection(".debug_aranges"))
    , info_(getSection(".debug_info"))
    , line_(getSection(".debug_line"))
    , line_str_(getSection(".debug_line_str"))
    , loclists_(getSection(".debug_loclists"))
    , ranges_(getSection(".debug_ranges"))
    , rnglists_(getSection(".debug_rnglists"))
    , str_(getSection(".debug_str"))
    , str_offsets_(getSection(".debug_str_offsets"))
{
    // Optional sections:
    //  - debugAranges_: for fast address range lookup.
    //     If missing .debug_info can be used - but it's much slower (linear
    //     scan).
    //  - debugRanges_ (DWARF 4) / debugRnglists_ (DWARF 5): non-contiguous
    //    address ranges of debugging information entries.
    //    Used for inline function address lookup.
    if (info_.empty() || abbrev_.empty() || line_.empty() || str_.empty())
    {
        elf_ = nullptr;
    }
}

Dwarf::Section::Section(std::string_view d) : is64_bit(false), data(d)
{
}


#define SAFE_CHECK(cond, ...) do { if (!(cond)) throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, __VA_ARGS__); } while (false)


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
    SAFE_CHECK(sp.size() >= sizeof(T), "underflow: expected bytes {}, got bytes {}", sizeof(T), sp.size());
    T x;
    memcpy(&x, sp.data(), sizeof(T));
    sp.remove_prefix(sizeof(T));
    return x;
}

// Read (bitwise) an unsigned number of N bytes (N in 1, 2, 3, 4).
template <size_t N>
uint64_t readU64(std::string_view & sp)
{
    SAFE_CHECK(sp.size() >= N, "underflow");
    uint64_t x = 0;
    if constexpr (std::endian::native == std::endian::little)
        memcpy(&x, sp.data(), N);
    else
        memcpy(reinterpret_cast<char*>(&x) + sizeof(uint64_t) - N, sp.data(), N);
    sp.remove_prefix(N);
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
uint64_t readOffset(std::string_view & sp, bool is64_bit)
{
    return is64_bit ? read<uint64_t>(sp) : read<uint32_t>(sp);
}

// Read "len" bytes
std::string_view readBytes(std::string_view & sp, uint64_t len)
{
    SAFE_CHECK(len <= sp.size(), "invalid string length: {} vs. {}", len, sp.size());
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

// Get a string from the section
std::string_view getStringFromStringSection(std::string_view section, uint64_t offset)
{
    SAFE_CHECK(offset < section.size(), "invalid section offset");
    std::string_view sp(section);
    sp.remove_prefix(offset);
    return readNullTerminated(sp);
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

std::string_view Dwarf::getSection(const char * name) const
{
    std::optional<Elf::Section> elf_section = elf_->findSectionByName(name);
    if (!elf_section)
        return {};

#ifdef SHF_COMPRESSED
    if (elf_section->header.sh_flags & SHF_COMPRESSED)
        return {};
#endif

    return { elf_section->begin(), elf_section->size()};
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
        auto attr = readAttribute(cu, die, spec, values);
        if (!f(attr))
        {
            return static_cast<size_t>(-1);
        }
    }
    return values.data() - info_.data();
}

Dwarf::Attribute Dwarf::readAttribute(const CompilationUnit & cu,
    const Die & die,
    AttributeSpec spec,
    std::string_view & info) const
{
    // DWARF 5 introduces new FORMs whose values are relative to some base attrs:
    // DW_AT_str_offsets_base, DW_AT_rnglists_base, DW_AT_addr_base.
    // Debug Fission DWARF 4 uses GNU DW_AT_GNU_ranges_base & DW_AT_GNU_addr_base.
    //
    // The order in which attributes appear in a CU is not defined.
    // The DW_AT_*_base attrs may appear after attributes that need them.
    // The DW_AT_*_base attrs are CU specific; so we read them just after
    // reading the CU header. During this first pass return empty values
    // when encountering a FORM that depends on DW_AT_*_base.
    auto get_string_using_offset_table = [&](uint64_t index)
    {
        if (!cu.str_offsets_base.has_value())
        {
            return std::string_view();
        }
        // DWARF 5: 7.26 String Offsets Table
        // The DW_AT_str_offsets_base attribute points to the first entry following
        // the header. The entries are indexed sequentially from this base entry,
        // starting from 0.
        auto sp = str_offsets_.substr(*cu.str_offsets_base + index * (cu.is64Bit ? sizeof(uint64_t) : sizeof(uint32_t)));
        uint64_t str_offset = readOffset(sp, cu.is64Bit);
        return getStringFromStringSection(str_, str_offset);
    };

    auto read_debug_addr = [&](uint64_t index)
    {
        if (!cu.addr_base.has_value())
        {
            return uint64_t(0);
        }
        // DWARF 5: 7.27 Address Table
        // The DW_AT_addr_base attribute points to the first entry following the
        // header. The entries are indexed sequentially from this base entry,
        // starting from 0.
        auto sp = addr_.substr(*cu.addr_base + index * sizeof(uint64_t));
        return read<uint64_t>(sp);
    };

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
            return {spec, die, static_cast<uint64_t>(readSLEB(info))};
        case DW_FORM_udata:
            [[fallthrough]];
        case DW_FORM_ref_udata:
            return {spec, die, readULEB(info)};
        case DW_FORM_flag:
            return {spec, die, read<uint8_t>(info)};
        case DW_FORM_flag_present:
            return {spec, die, 1ULL};
        case DW_FORM_sec_offset:
            [[fallthrough]];
        case DW_FORM_ref_addr:
            return {spec, die, readOffset(info, die.is64Bit)};
        case DW_FORM_string:
            return {spec, die, readNullTerminated(info)};
        case DW_FORM_strp:
            return {spec, die, getStringFromStringSection(str_, readOffset(info, die.is64Bit))};
        case DW_FORM_indirect: // form is explicitly specified
            // Update spec with the actual FORM.
            spec.form = readULEB(info);
            return readAttribute(cu, die, spec, info);

        // DWARF 5:
        case DW_FORM_implicit_const: // form is explicitly specified
            // For attributes with this form, the attribute specification contains a
            // third part, which is a signed LEB128 number. The value of this number
            // is used as the value of the attribute, and no value is stored in the
            // .debug_info section.
            return {spec, die, static_cast<uint64_t>(spec.implicitConst)};

        case DW_FORM_addrx:
            return {spec, die, read_debug_addr(readULEB(info))};
        case DW_FORM_addrx1:
            return {spec, die, read_debug_addr(readU64<1>(info))};
        case DW_FORM_addrx2:
            return {spec, die, read_debug_addr(readU64<2>(info))};
        case DW_FORM_addrx3:
            return {spec, die, read_debug_addr(readU64<3>(info))};
        case DW_FORM_addrx4:
            return {spec, die, read_debug_addr(readU64<4>(info))};

        case DW_FORM_line_strp:
            return {spec, die, getStringFromStringSection(line_str_, readOffset(info, die.is64Bit))};

        case DW_FORM_strx:
            return {spec, die, get_string_using_offset_table(readULEB(info))};
        case DW_FORM_strx1:
            return {spec, die, get_string_using_offset_table(readU64<1>(info))};
        case DW_FORM_strx2:
            return {spec, die, get_string_using_offset_table(readU64<2>(info))};
        case DW_FORM_strx3:
            return {spec, die, get_string_using_offset_table(readU64<3>(info))};
        case DW_FORM_strx4:
            return {spec, die, get_string_using_offset_table(readU64<4>(info))};

        case DW_FORM_rnglistx: {
            auto index = readULEB(info);
            if (!cu.rnglists_base.has_value())
            {
                return {spec, die, 0ULL};
            }
            const uint64_t offset_size = cu.is64Bit ? sizeof(uint64_t) : sizeof(uint32_t);
            auto sp = rnglists_.substr(*cu.rnglists_base + index * offset_size);
            auto offset = readOffset(sp, cu.is64Bit);
            return {spec, die, *cu.rnglists_base + offset};
        }

        case DW_FORM_loclistx: {
            auto index = readULEB(info);
            if (!cu.loclists_base.has_value())
            {
                return {spec, die, 0ULL};
            }
            const uint64_t offset_size = cu.is64Bit ? sizeof(uint64_t) : sizeof(uint32_t);
            auto sp = loclists_.substr(*cu.loclists_base + index * offset_size);
            auto offset = readOffset(sp, cu.is64Bit);
            return {spec, die, *cu.loclists_base + offset};
        }

        case DW_FORM_data16:
            return {spec, die, readBytes(info, 16)};

        case DW_FORM_ref_sup4:
        case DW_FORM_ref_sup8:
        case DW_FORM_strp_sup:
            SAFE_CHECK(false, "Unexpected DWARF5 supplimentary object files");

        default:
            SAFE_CHECK(false, "invalid attribute form");
    }
    return {spec, die, 0ULL};
}

// static
Dwarf::AttributeSpec Dwarf::readAttributeSpec(std::string_view & sp)
{
    Dwarf::AttributeSpec spec;
    spec.name = readULEB(sp);
    spec.form = readULEB(sp);
    if (spec.form == DW_FORM_implicit_const)
    {
        spec.implicitConst = readSLEB(sp);
    }
    return spec;
}

Dwarf::CompilationUnit Dwarf::getCompilationUnit(uint64_t offset) const
{
    // SAFE_CHECK(offset < info_.size(), "unexpected offset");
    CompilationUnit cu;
    std::string_view chunk(info_);
    cu.offset = offset;
    chunk.remove_prefix(offset);

    // 1) unit_length
    auto initial_length = read<uint32_t>(chunk);
    cu.is64Bit = (initial_length == uint32_t(-1));
    cu.size = cu.is64Bit ? read<uint64_t>(chunk) : initial_length;
    SAFE_CHECK(cu.size <= chunk.size(), "invalid chunk size");
    cu.size += cu.is64Bit ? 12 : 4;

    // 2) version
    cu.version = read<uint16_t>(chunk);
    SAFE_CHECK(cu.version >= 2 && cu.version <= 5, "invalid info version");

    if (cu.version == 5)
    {
        // DWARF5: 7.5.1.1 Full and Partial Compilation Unit Headers
        // 3) unit_type (new DWARF 5)
        cu.unit_type = read<uint8_t>(chunk);
        if (cu.unit_type != DW_UT_compile && cu.unit_type != DW_UT_skeleton)
        {
            return cu;
        }
        // 4) address_size
        cu.addr_size = read<uint8_t>(chunk);
        SAFE_CHECK(cu.addr_size == sizeof(uintptr_t), "invalid address size");

        // 5) debug_abbrev_offset
        cu.abbrev_offset = readOffset(chunk, cu.is64Bit);

        if (cu.unit_type == DW_UT_skeleton)
        {
            // 6) dwo_id
            read<uint64_t>(chunk);
        }
    }
    else
    {
        // DWARF4 has a single type of unit in .debug_info
        cu.unit_type = DW_UT_compile;
        // 3) debug_abbrev_offset
        cu.abbrev_offset = readOffset(chunk, cu.is64Bit);
        // 4) address_size
        cu.addr_size = read<uint8_t>(chunk);
        SAFE_CHECK(cu.addr_size == sizeof(uintptr_t), "invalid address size");
    }
    cu.first_die = chunk.data() - info_.data();
    if (cu.version < 5)
    {
        return cu;
    }

    Die die = getDieAtOffset(cu, cu.first_die);
    if (die.abbr.tag != DW_TAG_compile_unit)
    {
        return cu;
    }

    // Read the DW_AT_*_base attributes.
    // Attributes which use FORMs relative to these base attrs
    // will not have valid values during this first pass!
    forEachAttribute(
        cu,
        die,
        [&](const Attribute & attr)
        {
            switch (attr.spec.name) // NOLINT(bugprone-switch-missing-default-case)
            {
                case DW_AT_addr_base:
                case DW_AT_GNU_addr_base:
                    cu.addr_base = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_loclists_base:
                    cu.loclists_base = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_rnglists_base:
                case DW_AT_GNU_ranges_base:
                    cu.rnglists_base = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_str_offsets_base:
                    cu.str_offsets_base = std::get<uint64_t>(attr.attr_value);
                    break;
            }
            return true; // continue forEachAttribute
        });
    return cu;
}

// Finds the Compilation Unit starting at offset.
Dwarf::CompilationUnit Dwarf::findCompilationUnit(uint64_t targetOffset) const
{
    // SAFE_CHECK(targetOffset < info_.size(), "unexpected target address");
    uint64_t offset = 0;
    while (offset < info_.size())
    {
        std::string_view chunk(info_);
        chunk.remove_prefix(offset);

        auto initial_length = read<uint32_t>(chunk);
        auto is64_bit = (initial_length == static_cast<uint32_t>(-1));
        auto size = is64_bit ? read<uint64_t>(chunk) : initial_length;
        SAFE_CHECK(size <= chunk.size(), "invalid chunk size");
        size += is64_bit ? 12 : 4;

        if (offset + size > targetOffset)
        {
            break;
        }
        offset += size;
    }
    return getCompilationUnit(offset);
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

Dwarf::AttributeValue Dwarf::readAttributeValue(std::string_view & sp, uint64_t form, bool is64_bit) const
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
            return readOffset(sp, is64_bit);
        case DW_FORM_string:
            return readNullTerminated(sp);
        case DW_FORM_strp:
            return getStringFromStringSection(str_, readOffset(sp, is64_bit));
        case DW_FORM_indirect: // form is explicitly specified
            return readAttributeValue(sp, readULEB(sp), is64_bit);
        default:
            SAFE_CHECK(false, "invalid attribute form");
    }
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
    SAFE_CHECK(offset < info_.size(), "unexpected offset {}, info size {}", offset, info_.size());
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

std::optional<std::pair<std::optional<Dwarf::CompilationUnit>, uint64_t>> Dwarf::getReferenceAttribute(
    const CompilationUnit & cu, const Die & die, uint64_t attr_name) const
{
    bool found = false;
    uint64_t value;
    uint64_t form;
    forEachAttribute(cu, die, [&](const Attribute & attr)
    {
        if (attr.spec.name == attr_name)
        {
            found = true;
            value = std::get<uint64_t>(attr.attr_value);
            form = attr.spec.form;
            return false;
        }
        return true;
    });
    if (!found)
        return std::nullopt;
    switch (form)
    {
        case DW_FORM_ref1:
        case DW_FORM_ref2:
        case DW_FORM_ref4:
        case DW_FORM_ref8:
        case DW_FORM_ref_udata:
            return std::make_pair(std::nullopt, cu.offset + value);

        case DW_FORM_ref_addr:
            return std::make_pair(findCompilationUnit(value), value);

        case DW_FORM_ref_sig8:
            /// Currently we don't use this parser for types, so no need to support this.
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Type signatures are not supported (DIE at 0x{:x}, attr 0x{:x}).", die.offset, attr_name);

        case DW_FORM_ref_sup4:
        case DW_FORM_ref_sup8:
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Supplementary object files are not supported (DIE at 0x{:x}, attr 0x{:x}).", die.offset, attr_name);

        default:
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Unexpected form of attribute 0x{:x}: 0x{:x} (DIE at 0x{:x}).", attr_name, form, die.offset);
    }
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
    std::vector<SymbolizedFrame> & inline_frames,
    bool assume_in_cu_range) const
{
    Die die = getDieAtOffset(cu, cu.first_die);
    // Partial compilation unit (DW_TAG_partial_unit) is not supported.
    SAFE_CHECK(die.abbr.tag == DW_TAG_compile_unit, "expecting compile unit entry");

    // Offset in .debug_line for the line number VM program for this CU
    std::optional<uint64_t> line_offset = 0;
    std::string_view compilation_directory;
    std::optional<std::string_view> main_file_name;
    std::optional<uint64_t> base_addr_cu;

    std::optional<uint64_t> low_pc;
    std::optional<uint64_t> high_pc;
    std::optional<bool> is_high_pc_addr;
    std::optional<uint64_t> range_offset;

    forEachAttribute(cu, die, [&](const Attribute & attr)
    {
        switch (attr.spec.name) // NOLINT(bugprone-switch-missing-default-case)
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
            case DW_AT_entry_pc:
                // 2.17.1: historically DW_AT_low_pc was used. DW_AT_entry_pc was
                // introduced in DWARF3. Support either to determine the base address of
                // the CU.
                base_addr_cu = std::get<uint64_t>(attr.attr_value);
                break;
            case DW_AT_ranges:
                range_offset = std::get<uint64_t>(attr.attr_value);
                break;
            case DW_AT_low_pc:
                low_pc = std::get<uint64_t>(attr.attr_value);
                base_addr_cu = std::get<uint64_t>(attr.attr_value);
                break;
            case DW_AT_high_pc:
                // The value of the DW_AT_high_pc attribute can be
                // an address (DW_FORM_addr*) or an offset (DW_FORM_data*).
                is_high_pc_addr = attr.spec.form == DW_FORM_addr || //
                    attr.spec.form == DW_FORM_addrx || //
                    attr.spec.form == DW_FORM_addrx1 || //
                    attr.spec.form == DW_FORM_addrx2 || //
                    attr.spec.form == DW_FORM_addrx3 || //
                    attr.spec.form == DW_FORM_addrx4;
                high_pc = std::get<uint64_t>(attr.attr_value);
                break;
        }
        // Iterate through all attributes until find all above.
        return true;
    });

    /// Check if the address falls inside this unit's address ranges.
    if (!assume_in_cu_range && ((low_pc && high_pc) || range_offset))
    {
        bool pc_match = low_pc && high_pc && is_high_pc_addr && address >= *low_pc
            && (address < (*is_high_pc_addr ? *high_pc : *low_pc + *high_pc));
        bool range_match = range_offset && isAddrInRangeList(cu, address, base_addr_cu, range_offset.value(), cu.addr_size);
        if (!pc_match && !range_match)
        {
            return false;
        }
    }

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
    LineNumberVM line_vm(line_section, compilation_directory, str_, line_str_);

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
                info.has_file_and_line = true;
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

void Dwarf::findSubProgramDieForAddress(const CompilationUnit & cu,
    const Die & die,
    uint64_t address,
    std::optional<uint64_t> base_addr_cu,
    Die & subprogram) const
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
                switch (attr.spec.name) // NOLINT(bugprone-switch-missing-default-case)
                {
                    case DW_AT_ranges:
                        range_offset = std::get<uint64_t>(attr.attr_value);
                        break;
                    case DW_AT_low_pc:
                        low_pc = std::get<uint64_t>(attr.attr_value);
                        break;
                    case DW_AT_high_pc:
                        // The value of the DW_AT_high_pc attribute can be
                        // an address (DW_FORM_addr*) or an offset (DW_FORM_data*).
                        is_high_pc_addr = attr.spec.form == DW_FORM_addr || //
                            attr.spec.form == DW_FORM_addrx || //
                            attr.spec.form == DW_FORM_addrx1 || //
                            attr.spec.form == DW_FORM_addrx2 || //
                            attr.spec.form == DW_FORM_addrx3 || //
                            attr.spec.form == DW_FORM_addrx4;
                        high_pc = std::get<uint64_t>(attr.attr_value);
                        break;
                }
                // Iterate through all attributes until find all above.
                return true;
            });
            bool pc_match = low_pc && high_pc && is_high_pc_addr && address >= *low_pc
                && (address < (*is_high_pc_addr ? *high_pc : *low_pc + *high_pc));
            bool range_match = range_offset && isAddrInRangeList(cu, address, base_addr_cu, range_offset.value(), cu.addr_size);
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
            switch (attr.spec.name) // NOLINT(bugprone-switch-missing-default-case)
            {
                case DW_AT_ranges:
                    range_offset = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_low_pc:
                    low_pc = std::get<uint64_t>(attr.attr_value);
                    break;
                case DW_AT_high_pc:
                    // The value of the DW_AT_high_pc attribute can be
                    // an address (DW_FORM_addr*) or an offset (DW_FORM_data*).
                    is_high_pc_addr = attr.spec.form == DW_FORM_addr || //
                        attr.spec.form == DW_FORM_addrx || //
                        attr.spec.form == DW_FORM_addrx1 || //
                        attr.spec.form == DW_FORM_addrx2 || //
                        attr.spec.form == DW_FORM_addrx3 || //
                        attr.spec.form == DW_FORM_addrx4;
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
        bool range_match = range_offset && isAddrInRangeList(cu, address, base_addr_cu, range_offset.value(), cu.addr_size);
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

        auto get_function_name = [&](const CompilationUnit & srcu, uint64_t die_offset)
        {
            Die die_to_look_for_name = getDieAtOffset(srcu, die_offset);

            // Jump to the actual function definition instead of declaration for name
            // and line info.
            // DW_AT_specification: Incomplete, non-defining, or separate declaration
            // corresponding to a declaration
            auto def = getReferenceAttribute(srcu, die_to_look_for_name, DW_AT_specification);
            if (def.has_value())
            {
                auto [def_cu, def_offset] = std::move(def.value());
                const CompilationUnit & def_cu_ref = def_cu.has_value() ? def_cu.value() : srcu;
                die_to_look_for_name = getDieAtOffset(def_cu_ref, def_offset);
            }

            std::string_view name;

            // The file and line will be set in the next inline subroutine based on
            // its DW_AT_call_file and DW_AT_call_line.
            forEachAttribute(srcu, die_to_look_for_name, [&](const Attribute & attr)
            {
                switch (attr.spec.name) // NOLINT(bugprone-switch-missing-default-case)
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
            : get_function_name(findCompilationUnit(*abstract_origin), *abstract_origin);

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
            auto unit = getCompilationUnit(offset);
            if (unit.unit_type != DW_UT_compile && unit.unit_type != DW_UT_skeleton)
            {
                return false;
            }
            findLocation(address, mode, unit, locationInfo, inline_frames, /*assume_in_cu_range*/ true);
            return locationInfo.has_file_and_line;
        }
        if (mode == LocationInfoMode::FAST)
        {
            // NOTE: Clang (when using -gdwarf-aranges) doesn't generate entries
            // in .debug_aranges for some functions, but always generates
            // .debug_info entries.  Scanning .debug_info is slow, so fall back to
            // it only if such behavior is requested via LocationInfoMode.
            return false;
        }

        SAFE_CHECK(mode == LocationInfoMode::FULL || mode == LocationInfoMode::FULL_WITH_INLINE, "unexpected mode");
        // Fall back to the linear scan.
    }

    // Slow path (linear scan): Iterate over all .debug_info entries
    // and look for the address in each compilation unit.
    uint64_t offset = 0;
    while (offset < info_.size() && !locationInfo.has_file_and_line)
    {
        auto unit = getCompilationUnit(offset);
        offset += unit.size;
        if (unit.unit_type != DW_UT_compile && unit.unit_type != DW_UT_skeleton)
        {
            continue;
        }
        findLocation(address, mode, unit, locationInfo, inline_frames, /*assume_in_cu_range*/ false);
    }

    return locationInfo.has_file_and_line;
}

bool Dwarf::isAddrInRangeList(const CompilationUnit & cu,
    uint64_t address,
    std::optional<uint64_t> base_addr,
    size_t offset,
    uint8_t addr_size) const
{
    SAFE_CHECK(addr_size == 4 || addr_size == 8, "wrong address size");
    if (cu.version <= 4 && !ranges_.empty())
    {
        const bool is64_bit_addr = addr_size == 8;
        std::string_view sp = ranges_;
        sp.remove_prefix(offset);
        const uint64_t max_addr = is64_bit_addr ? std::numeric_limits<uint64_t>::max() : std::numeric_limits<uint32_t>::max();
        while (!sp.empty())
        {
            uint64_t begin = readOffset(sp, is64_bit_addr);
            uint64_t end = readOffset(sp, is64_bit_addr);
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
            // range list. If there is no such selection entry, then the applicable
            // base address defaults to the base address of the compilation unit.
            if (base_addr && address >= begin + *base_addr && address < end + *base_addr)
            {
                return true;
            }
        }
    }

    if (cu.version == 5 && !rnglists_.empty() && cu.addr_base.has_value())
    {
        auto rnglists = rnglists_;
        rnglists.remove_prefix(offset);

        while (!rnglists.empty())
        {
            auto kind = read<uint8_t>(rnglists);
            switch (kind)
            {
                case DW_RLE_end_of_list:
                    return false;
                case DW_RLE_base_addressx: {
                    auto index = readULEB(rnglists);
                    auto sp = addr_.substr(*cu.addr_base + index * sizeof(uint64_t));
                    base_addr = read<uint64_t>(sp);
                }
                break;

                case DW_RLE_startx_endx: {
                    auto index_start = readULEB(rnglists);
                    auto index_end = readULEB(rnglists);
                    auto sp_start = addr_.substr(*cu.addr_base + index_start * sizeof(uint64_t));
                    auto start = read<uint64_t>(sp_start);

                    auto sp_end = addr_.substr(*cu.addr_base + index_end * sizeof(uint64_t));
                    auto end = read<uint64_t>(sp_end);
                    if (address >= start && address < end)
                    {
                        return true;
                    }
                }
                break;

                case DW_RLE_startx_length: {
                    auto index_start = readULEB(rnglists);
                    auto length = readULEB(rnglists);
                    auto sp_start = addr_.substr(*cu.addr_base + index_start * sizeof(uint64_t));
                    auto start = read<uint64_t>(sp_start);

                    auto end = start + length;
                    if (start != end && address >= start && address < end)
                    {
                        return true;
                    }
                }
                break;

                case DW_RLE_offset_pair: {
                    auto offset_start = readULEB(rnglists);
                    auto offset_end = readULEB(rnglists);
                    if (base_addr && address >= (*base_addr + offset_start) && address < (*base_addr + offset_end))
                    {
                        return true;
                    }
                }
                break;

                case DW_RLE_base_address:
                    base_addr = read<uint64_t>(rnglists);
                    break;

                case DW_RLE_start_end: {
                    uint64_t start = read<uint64_t>(rnglists);
                    uint64_t end = read<uint64_t>(rnglists);
                    if (address >= start && address < end)
                    {
                        return true;
                    }
                }
                break;

                case DW_RLE_start_length: {
                    uint64_t start = read<uint64_t>(rnglists);
                    uint64_t end = start + readULEB(rnglists);
                    if (address >= start && address < end)
                    {
                        return true;
                    }
                }
                break;

                default:
                    SAFE_CHECK(false, "Unexpected debug_rnglists entry kind");
            }
        }
    }
    return false;
}


Dwarf::LineNumberVM::LineNumberVM(
    std::string_view data,
    std::string_view compilationDirectory,
    std::string_view debugStr,
    std::string_view debugLineStr)
    : compilationDirectory_(compilationDirectory)
    , debugStr_(debugStr)
    , debugLineStr_(debugLineStr)
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

struct LineNumberAttribute
{
    uint64_t content_type_code;
    uint64_t form_code;
    std::variant<uint64_t, std::string_view> attr_value;
};

LineNumberAttribute readLineNumberAttribute(
    bool is64_bit, std::string_view & format, std::string_view & entries, std::string_view debugStr, std::string_view debugLineStr)
{
    uint64_t content_type_code = readULEB(format);
    uint64_t form_code = readULEB(format);
    std::variant<uint64_t, std::string_view> attr_value;

    switch (content_type_code)
    {
        case DW_LNCT_path: {
            switch (form_code)
            {
                case DW_FORM_string:
                    attr_value = readNullTerminated(entries);
                    break;
                case DW_FORM_line_strp: {
                    auto off = readOffset(entries, is64_bit);
                    attr_value = getStringFromStringSection(debugLineStr, off);
                }
                break;
                case DW_FORM_strp:
                    attr_value = getStringFromStringSection(debugStr, readOffset(entries, is64_bit));
                    break;
                case DW_FORM_strp_sup:
                    SAFE_CHECK(false, "Unexpected DW_FORM_strp_sup");
                    break;
                default:
                    SAFE_CHECK(false, "Unexpected form for DW_LNCT_path");
                    break;
            }
        }
        break;

        case DW_LNCT_directory_index: {
            switch (form_code)
            {
                case DW_FORM_data1:
                    attr_value = read<uint8_t>(entries);
                    break;
                case DW_FORM_data2:
                    attr_value = read<uint16_t>(entries);
                    break;
                case DW_FORM_udata:
                    attr_value = readULEB(entries);
                    break;
                default:
                    SAFE_CHECK(false, "Unexpected form for DW_LNCT_directory_index");
                    break;
            }
        }
        break;

        case DW_LNCT_timestamp: {
            switch (form_code)
            {
                case DW_FORM_udata:
                    attr_value = readULEB(entries);
                    break;
                case DW_FORM_data4:
                    attr_value = read<uint32_t>(entries);
                    break;
                case DW_FORM_data8:
                    attr_value = read<uint64_t>(entries);
                    break;
                case DW_FORM_block:
                    attr_value = readBytes(entries, readULEB(entries));
                    break;
                default:
                    SAFE_CHECK(false, "Unexpected form for DW_LNCT_timestamp");
            }
        }
        break;

        case DW_LNCT_size: {
            switch (form_code)
            {
                case DW_FORM_udata:
                    attr_value = readULEB(entries);
                    break;
                case DW_FORM_data1:
                    attr_value = read<uint8_t>(entries);
                    break;
                case DW_FORM_data2:
                    attr_value = read<uint16_t>(entries);
                    break;
                case DW_FORM_data4:
                    attr_value = read<uint32_t>(entries);
                    break;
                case DW_FORM_data8:
                    attr_value = read<uint64_t>(entries);
                    break;
                default:
                    SAFE_CHECK(false, "Unexpected form for DW_LNCT_size");
                    break;
            }
        }
        break;

        case DW_LNCT_MD5: {
            switch (form_code)
            {
                case DW_FORM_data16:
                    attr_value = readBytes(entries, 16);
                    break;
                default:
                    SAFE_CHECK(false, "Unexpected form for DW_LNCT_MD5");
                    break;
            }
        }
        break;

        default:
            // TODO: skip over vendor data as specified by the form instead.
            SAFE_CHECK(false, "Unexpected vendor content type code");
            break;
    }
    return {
        .content_type_code = content_type_code,
        .form_code = form_code,
        .attr_value = attr_value,
    };
}

void Dwarf::LineNumberVM::init()
{
    version_ = read<uint16_t>(data_);
    SAFE_CHECK(version_ >= 2 && version_ <= 5, "invalid version in line number VM: {}", version_);
    if (version_ == 5)
    {
        auto address_size = read<uint8_t>(data_);
        SAFE_CHECK(address_size == sizeof(uintptr_t), "Unexpected Line Number Table address_size");
        auto segment_selector_size = read<uint8_t>(data_);
        SAFE_CHECK(segment_selector_size == 0, "Segments not supported");
    }
    uint64_t header_length = readOffset(data_, is64Bit_);
    SAFE_CHECK(header_length <= data_.size(), "invalid line number VM header length");
    std::string_view header(data_.data(), header_length);
    data_ = std::string_view(header.end(), data_.end() - header.end());

    minLength_ = read<uint8_t>(header);
    if (version_ >= 4)
    { // Version 2 and 3 records don't have this
        uint8_t max_ops_per_instruction = read<uint8_t>(header);
        SAFE_CHECK(max_ops_per_instruction == 1, "VLIW not supported");
    }
    defaultIsStmt_ = read<uint8_t>(header);
    lineBase_ = read<int8_t>(header); // yes, signed
    lineRange_ = read<uint8_t>(header);
    opcodeBase_ = read<uint8_t>(header);
    SAFE_CHECK(opcodeBase_ != 0, "invalid opcode base");
    standardOpcodeLengths_ = reinterpret_cast<const uint8_t *>(header.data());
    header.remove_prefix(opcodeBase_ - 1);

    if (version_ <= 4)
    {
        // We don't want to use heap, so we don't keep an unbounded amount of state.
        // We'll just skip over include directories and file names here, and
        // we'll loop again when we actually need to retrieve one.
        std::string_view sp;
        const char * tmp = header.data();
        v4_.includeDirectoryCount = 0;
        while (!(sp = readNullTerminated(header)).empty())
        {
            ++v4_.includeDirectoryCount;
        }
        v4_.includeDirectories = {tmp, header.data()};

        tmp = header.data();
        FileName fn;
        v4_.fileNameCount = 0;
        while (readFileName(header, fn))
        {
            ++v4_.fileNameCount;
        }
        v4_.fileNames = {tmp, header.data()};
    }
    else if (version_ == 5)
    {
        v5_.directoryEntryFormatCount = read<uint8_t>(header);
        const char * tmp = header.data();
        for (uint8_t i = 0; i < v5_.directoryEntryFormatCount; i++)
        {
            // A sequence of directory entry format descriptions. Each description
            // consists of a pair of ULEB128 values:
            readULEB(header); // A content type code
            readULEB(header); // A form code using the attribute form codes
        }
        v5_.directoryEntryFormat = {tmp, header.data()};
        v5_.directoriesCount = readULEB(header);
        tmp = header.data();
        for (uint64_t i = 0; i < v5_.directoriesCount; i++)
        {
            std::string_view format = v5_.directoryEntryFormat;
            for (uint8_t f = 0; f < v5_.directoryEntryFormatCount; f++)
            {
                readLineNumberAttribute(is64Bit_, format, header, debugStr_, debugLineStr_);
            }
        }
        v5_.directories = {tmp, header.data()};

        v5_.fileNameEntryFormatCount = read<uint8_t>(header);
        tmp = header.data();
        for (uint8_t i = 0; i < v5_.fileNameEntryFormatCount; i++)
        {
            // A sequence of file entry format descriptions. Each description
            // consists of a pair of ULEB128 values:
            readULEB(header); // A content type code
            readULEB(header); // A form code using the attribute form codes
        }
        v5_.fileNameEntryFormat = {tmp, header.data()};
        v5_.fileNamesCount = readULEB(header);
        tmp = header.data();
        for (uint64_t i = 0; i < v5_.fileNamesCount; i++)
        {
            std::string_view format = v5_.fileNameEntryFormat;
            for (uint8_t f = 0; f < v5_.fileNameEntryFormatCount; f++)
            {
                readLineNumberAttribute(is64Bit_, format, header, debugStr_, debugLineStr_);
            }
        }
        v5_.fileNames = {tmp, header.data()};
    }
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
    if (version_ <= 4)
    {
        SAFE_CHECK(index != 0, "invalid file index 0");
        FileName fn;
        if (index <= v4_.fileNameCount)
        {
            std::string_view file_names = v4_.fileNames;
            for (; index; --index)
            {
                if (!readFileName(file_names, fn))
                {
                    abort();
                }
            }
            return fn;
        }

        index -= v4_.fileNameCount;

        std::string_view program = data_;
        for (; index; --index)
        {
            SAFE_CHECK(nextDefineFile(program, fn), "invalid file index");
        }

        return fn;
    }

    FileName fn;
    SAFE_CHECK(index < v5_.fileNamesCount, "invalid file index");
    std::string_view file_names = v5_.fileNames;
    for (uint64_t i = 0; i < v5_.fileNamesCount; i++)
    {
        std::string_view format = v5_.fileNameEntryFormat;
        for (uint8_t f = 0; f < v5_.fileNameEntryFormatCount; f++)
        {
            auto attr = readLineNumberAttribute(is64Bit_, format, file_names, debugStr_, debugLineStr_);
            if (i == index)
            {
                switch (attr.content_type_code) // NOLINT(bugprone-switch-missing-default-case)
                {
                    case DW_LNCT_path:
                        fn.relativeName = std::get<std::string_view>(attr.attr_value);
                        break;
                    case DW_LNCT_directory_index:
                        fn.directoryIndex = std::get<uint64_t>(attr.attr_value);
                        break;
                }
            }
        }
    }
    return fn;
}

std::string_view Dwarf::LineNumberVM::getIncludeDirectory(uint64_t index) const
{
    if (version_ <= 4)
    {
        if (index == 0)
        {
            // In DWARF <= 4 the current directory is not represented in the
            // directories field and a directory index of 0 implicitly referred to
            // that directory as found in the DW_AT_comp_dir attribute of the
            // compilation unit debugging information entry.
            return {};
        }

        SAFE_CHECK(index <= v4_.includeDirectoryCount, "invalid include directory");

        std::string_view include_directories = v4_.includeDirectories;
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

    SAFE_CHECK(index < v5_.directoriesCount, "invalid file index");
    std::string_view directories = v5_.directories;
    for (uint64_t i = 0; i < v5_.directoriesCount; i++)
    {
        std::string_view format = v5_.directoryEntryFormat;
        for (uint8_t f = 0; f < v5_.directoryEntryFormatCount; f++)
        {
            auto attr = readLineNumberAttribute(is64Bit_, format, directories, debugStr_, debugLineStr_);
            if (i == index && attr.content_type_code == DW_LNCT_path)
            {
                return std::get<std::string_view>(attr.attr_value);
            }
        }
    }
    // This could only happen if DWARF5's directory_entry_format doesn't contain
    // a DW_LNCT_path. Highly unlikely, but we shouldn't crash.
    return std::string_view("<directory not found>");
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
            SAFE_CHECK(version_ < 5, "DW_LNE_define_file deprecated in DWARF5");
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
        // the others are vendor extensions, and we should ignore them.
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
            default:
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

    switch (extended_opcode) // NOLINT(bugprone-switch-missing-default-case)
    {
        case DW_LNE_end_sequence:
            return END;
        case DW_LNE_set_address:
            address_ = read<uintptr_t>(program);
            return CONTINUE;
        case DW_LNE_define_file:
            SAFE_CHECK(version_ < 5, "DW_LNE_define_file deprecated in DWARF5");
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

Dwarf::Path Dwarf::LineNumberVM::getFullFileName(uint64_t index) const
{
    auto fn = getFileName(index);
    // DWARF <= 4: the current dir is not represented in the CU's Line Number
    // Program Header and relies on the CU's DW_AT_comp_dir.
    // DWARF 5: the current directory is explicitly present.
    const std::string_view base_dir = version_ == 5 ? "" : compilationDirectory_;
    return Path(base_dir, getIncludeDirectory(fn.directoryIndex), fn.relativeName);
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
                //
                // NOTE: In DWARF <= 4 the file register is non-zero.
                //   See DWARF 4: 6.2.4 The Line Number Program Header
                //   "The line number program assigns numbers to each of the file
                //   entries in order, beginning with 1, and uses those numbers instead
                //   of file names in the file register."
                // DWARF 5 has a different include directory/file header and 0 is valid.
                if (version_ <= 4 && prev_file == 0)
                {
                    return false;
                }
                file = getFullFileName(prev_file);
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
