#include "DWARFBlockInputFormat.h"
#if USE_DWARF_PARSER && defined(__ELF__) && !defined(OS_FREEBSD)

#include <llvm/DebugInfo/DWARF/DWARFFormValue.h>
#include <llvm/DebugInfo/DWARF/DWARFDebugRnglists.h>
#include <llvm/DebugInfo/DWARF/DWARFDebugRangeList.h>
#include <llvm/BinaryFormat/Dwarf.h>

#include <base/hex.h>
#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnUnique.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WithFileName.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>

namespace CurrentMetrics
{
    extern const Metric DWARFReaderThreads;
    extern const Metric DWARFReaderThreadsActive;
    extern const Metric DWARFReaderThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_ELF;
    extern const int CANNOT_PARSE_DWARF;
}

enum DwarfColumn
{
    COL_OFFSET,
    COL_SIZE,
    COL_TAG,
    COL_UNIT_NAME,
    COL_UNIT_OFFSET,

    COL_ANCESTOR_TAGS,
    COL_ANCESTOR_OFFSETS,

    /// A few very common attributes get their own columns, just for convenience.
    /// We put their values *both* in the dedicated columns and in the attr_str/attr_int arrays.
    /// This duplication wastes considerable time and space (tens of percent), but I can't think of
    /// an alternative that wouldn't be really inconvenient or confusing:
    ///  * omitting these attributes from the arrays would make collecting attribute stats inconvenient,
    ///    and would lose information about the form of the attribute,
    ///  * using empty value for the attribute would be confusing and error-prone, e.g. when collecting stats
    ///    about all attribute values the user would need to add these columns too, somehow,
    ///  * not having these dedicated columns would make it inconvenient to look up entry name/file/line.
    ///    (But maybe that's fine? I.e. maybe it's not very commonly used and maybe the array lookup is not that inconvenient? Idk.)

    COL_NAME,
    COL_LINKAGE_NAME,
    COL_DECL_FILE,
    COL_DECL_LINE,
    COL_RANGES,

    COL_ATTR_NAME,
    COL_ATTR_FORM,
    COL_ATTR_INT,
    COL_ATTR_STR,

    COL_COUNT,
};

static NamesAndTypesList getHeaderForDWARF()
{
    std::vector<NameAndTypePair> cols(COL_COUNT);
    cols[COL_OFFSET] = {"offset", std::make_shared<DataTypeUInt64>()};
    cols[COL_SIZE] = {"size", std::make_shared<DataTypeUInt32>()};
    cols[COL_TAG] = {"tag", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_UNIT_NAME] = {"unit_name", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_UNIT_OFFSET] = {"unit_offset", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>())};
    cols[COL_ANCESTOR_TAGS] = {"ancestor_tags", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ANCESTOR_OFFSETS] = {"ancestor_offsets", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    cols[COL_NAME] = {"name", std::make_shared<DataTypeString>()};
    cols[COL_LINKAGE_NAME] = {"linkage_name", std::make_shared<DataTypeString>()};
    cols[COL_DECL_FILE] = {"decl_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())};
    cols[COL_DECL_LINE] = {"decl_line", std::make_shared<DataTypeUInt32>()};
    cols[COL_RANGES] = {"ranges", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()}))};
    cols[COL_ATTR_NAME] = {"attr_name", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ATTR_FORM] = {"attr_form", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    cols[COL_ATTR_INT] = {"attr_int", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())};
    cols[COL_ATTR_STR] = {"attr_str", std::make_shared<DataTypeArray>(std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()))};
    return NamesAndTypesList(cols.begin(), cols.end());
}

static const std::unordered_map<std::string, size_t> & getColumnNameToIdx()
{
    static std::once_flag once;
    static std::unordered_map<std::string, size_t> name_to_idx;
    std::call_once(once, [&] {
        size_t i = 0;
        for (const auto & c : getHeaderForDWARF())
        {
            name_to_idx.emplace(c.name, i);
            ++i;
        }
    });
    return name_to_idx;
}

DWARFBlockInputFormat::UnitState::UnitState(llvm::DWARFUnit * u)
    : dwarf_unit(u), end_offset(dwarf_unit->getNextUnitOffset())
    , offset(dwarf_unit->getOffset() + dwarf_unit->getHeaderSize())
{
    /// This call is not thread safe, so we do it during initialization.
    abbrevs = dwarf_unit->getAbbreviations();
    if (abbrevs == nullptr)
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Couldn't find abbreviation set for unit at offset {}", dwarf_unit->getOffset());

    /// This call initializes some data structures inside DWARFUnit that are needed for parsing attributes.
    auto err = u->tryExtractDIEsIfNeeded(/*CUDieOnly*/ true);
    if (err)
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Failed to parse compilation unit entry: {}", llvm::toString(std::move(err)));
}

static llvm::StringRef removePrefix(llvm::StringRef s, size_t prefix_len)
{
    if (s.size() >= prefix_len)
        s = llvm::StringRef(s.data() + prefix_len, s.size() - prefix_len);
    return s;
}

template <typename C>
static void append(C & col, llvm::StringRef s)
{
    col->insertData(s.data(), s.size());
}

DWARFBlockInputFormat::DWARFBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_, size_t num_threads_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_), num_threads(num_threads_)
{
    auto tag_names = ColumnString::create();
    constexpr std::string_view DW_TAG_ = "DW_TAG_";
    tag_names->insertDefault();
    for (uint32_t tag = 1; tag <= UINT16_MAX; ++tag)
    {
        auto name = removePrefix(llvm::dwarf::TagString(tag), DW_TAG_.size());
        if (name.empty())
        {
            /// ColumnUnique requires values to be unique, even unused ones.
            append(tag_names, "unknown: " + std::to_string(tag));
        }
        else
        {
            append(tag_names, name);
        }
    }
    tag_dict_column = ColumnUnique<ColumnString>::create(std::move(tag_names), /*is_nullable*/ false);

    auto attr_names = ColumnString::create();
    constexpr std::string_view DW_AT_ = "DW_AT_";
    attr_names->insertDefault();
    for (uint32_t attr = 1; attr <= UINT16_MAX; ++attr)
    {
        auto name = removePrefix(llvm::dwarf::AttributeString(attr), DW_AT_.size());
        if (name.empty())
            append(attr_names, "unknown: " + std::to_string(attr));
        else
            append(attr_names, name);
    }
    attr_name_dict_column = ColumnUnique<ColumnString>::create(std::move(attr_names), /*is_nullable*/ false);

    auto attr_forms = ColumnString::create();
    constexpr std::string_view DW_FORM_ = "DW_FORM_";
    attr_forms->insertDefault();
    for (uint32_t form = 1; form <= UINT16_MAX; ++form)
    {
        auto name = removePrefix(llvm::dwarf::FormEncodingString(form), DW_FORM_.size());
        if (name.empty())
            append(attr_forms, "unknown: " + std::to_string(form));
        else
            append(attr_forms, name);
    }
    attr_form_dict_column = ColumnUnique<ColumnString>::create(std::move(attr_forms), /*is_nullable*/ false);
}

DWARFBlockInputFormat::~DWARFBlockInputFormat()
{
    stopThreads();
}

void DWARFBlockInputFormat::initELF()
{
    /// If it's a local file, mmap it.
    if (ReadBufferFromFileBase * file_in = dynamic_cast<ReadBufferFromFileBase *>(in))
    {
        size_t offset = 0;
        if (file_in->isRegularLocalFile(&offset) && offset == 0)
        {
            elf.emplace(file_in->getFileName());
            return;
        }
    }

    /// If can't mmap, read the entire file into memory.
    /// We could read just the .debug_* sections, but typically they take up most of the binary anyway (60% for clickhouse debug build).
    {
        auto buf = WriteBufferFromVector<PODArray<char>>(file_contents);
        copyData(*in, buf, is_stopped);
    }
    elf.emplace(file_contents.data(), file_contents.size(), "<input>");
}

void DWARFBlockInputFormat::initializeIfNeeded()
{
    if (elf.has_value())
        return;

    LOG_DEBUG(getLogger("DWARF"), "Opening ELF");
    initELF();
    if (is_stopped)
        return;

    auto info_section = elf->findSectionByName(".debug_info");
    if (!info_section.has_value())
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "No .debug_info section");
    auto abbrev_section = elf->findSectionByName(".debug_abbrev");
    if (!abbrev_section.has_value())
        throw Exception(ErrorCodes::CANNOT_PARSE_ELF, "No .debug_abbrev section");
    LOG_DEBUG(getLogger("DWARF"), ".debug_abbrev is {:.3f} MiB, .debug_info is {:.3f} MiB", abbrev_section->size() * 1. / (1 << 20), info_section->size() * 1. / (1 << 20));

    /// (The StringRef points into Elf's mmap of the whole file, or into file_contents.)
    extractor.emplace(llvm::StringRef(info_section->begin(), info_section->size()), /*IsLittleEndian*/ true, /*AddressSize*/ 8);

    if (auto line_section = elf->findSectionByName(".debug_line"))
        debug_line_extractor.emplace(llvm::StringRef(line_section->begin(), line_section->size()), /*IsLittleEndian*/ true, /*AddressSize*/ 8);
    if (auto addr_section = elf->findSectionByName(".debug_addr"))
        debug_addr_section.emplace(addr_section->begin(), addr_section->size());
    if (auto rnglists_section = elf->findSectionByName(".debug_rnglists"))
        debug_rnglists_extractor.emplace(llvm::StringRef(rnglists_section->begin(), rnglists_section->size()), /*IsLittleEndian*/ true, /*AddressSize*/ 8);
    if (auto ranges_section = elf->findSectionByName(".debug_ranges"))
        debug_ranges_extractor.emplace(llvm::StringRef(ranges_section->begin(), ranges_section->size()), /*IsLittleEndian*/ true, /*AddressSize*/ 8);

    llvm::StringMap<std::unique_ptr<llvm::MemoryBuffer>> sections;
    elf->iterateSections([&](const Elf::Section & section, size_t /*idx*/)
        {
            std::string name = section.name();
            std::string name_without_dot = name.starts_with(".") ? name.substr(1) : name;
            sections.try_emplace(name_without_dot, llvm::MemoryBuffer::getMemBuffer(
                llvm::StringRef(section.begin(), section.size()), /*BufferName*/ name, /*RequiresNullTerminator*/ false));
            return false;
        });
    dwarf_context = llvm::DWARFContext::create(sections, /*AddrSize*/ 8);

    for (std::unique_ptr<llvm::DWARFUnit> & unit : dwarf_context->info_section_units())
        units_queue.emplace_back(unit.get());

    LOG_DEBUG(getLogger("DWARF"), "{} units, reading in {} threads", units_queue.size(), num_threads);

    pool.emplace(CurrentMetrics::DWARFReaderThreads, CurrentMetrics::DWARFReaderThreadsActive, CurrentMetrics::DWARFReaderThreadsScheduled, num_threads);
    for (size_t i = 0; i < num_threads; ++i)
        pool->scheduleOrThrowOnError(
            [this, thread_group = CurrentThread::getGroup()]()
            {
                try
                {
                    ThreadGroupSwitcher switcher(thread_group, "DWARFDecoder");

                    std::unique_lock lock(mutex);
                    while (!units_queue.empty() && !is_stopped)
                    {
                        if (delivery_queue.size() > num_threads)
                        {
                            wake_up_threads.wait(lock);
                            continue;
                        }
                        UnitState unit = std::move(units_queue.front());
                        units_queue.pop_front();
                        ++units_in_progress;

                        lock.unlock();

                        size_t offset_before = unit.offset;
                        Chunk chunk = parseEntries(unit);
                        size_t offset_after = unit.offset;

                        lock.lock();

                        --units_in_progress;
                        if (chunk)
                        {
                            delivery_queue.emplace_back(std::move(chunk), offset_after - offset_before);
                            deliver_chunk.notify_one();
                        }
                        if (!unit.eof())
                            units_queue.push_front(std::move(unit));
                    }
                }
                catch (...)
                {
                    std::lock_guard lock(mutex);
                    background_exception = std::current_exception();
                    deliver_chunk.notify_all();
                }
            });
}

void DWARFBlockInputFormat::stopThreads()
{
    {
        std::unique_lock lock(mutex); // required even if is_stopped is atomic
        is_stopped = true;
    }
    wake_up_threads.notify_all();
    if (pool)
        pool->wait();
}

static inline void throwIfError(llvm::Error & e, const char * what)
{
    if (!e)
        return;
    throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Failed to parse {}: {}", what, llvm::toString(std::move(e)));
}

llvm::DWARFFormValue DWARFBlockInputFormat::parseAttribute(
    const llvm::DWARFAbbreviationDeclaration::AttributeSpec & attr, uint64_t * offset,
    const UnitState & unit) const
{
    auto val = llvm::DWARFFormValue::createFromSValue(
        attr.Form, attr.isImplicitConst() ? attr.getImplicitConstValue() : 0);
    if (!val.extractValue(*extractor, offset, unit.dwarf_unit->getFormParams(), unit.dwarf_unit))
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF,
            "Failed to parse attribute {} of form {} at offset {}",
                llvm::dwarf::AttributeString(attr.Attr).operator std::string_view(), attr.Form, *offset);
    return val;
}

void DWARFBlockInputFormat::skipAttribute(
    const llvm::DWARFAbbreviationDeclaration::AttributeSpec & attr, uint64_t * offset,
    const UnitState & unit) const
{
    if (attr.Form == llvm::dwarf::DW_FORM_strx3)
    {
        /// DWARFFormValue::skipValue() fails on DW_FORM_strx3 because the `switch` statement is
        /// missing this form for some reason. Maybe it's a bug in llvm.
        /// Use extractValue() to work around.
        parseAttribute(attr, offset, unit);
    }
    else
    {
        if (!llvm::DWARFFormValue::skipValue(
            attr.Form, *extractor, offset, unit.dwarf_unit->getFormParams()))
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF,
                    "Failed to skip attribute {} of form {} at offset {}",
                    llvm::dwarf::AttributeString(attr.Attr).operator std::string_view(), attr.Form, *offset);
    }
}

uint64_t DWARFBlockInputFormat::parseAddress(llvm::dwarf::Attribute attr, const llvm::DWARFFormValue & val, const UnitState & unit)
{
    if (val.getForm() == llvm::dwarf::DW_FORM_addr)
        return val.getRawUValue();
    if (val.getForm() == llvm::dwarf::DW_FORM_addrx ||
        (val.getForm() >= llvm::dwarf::DW_FORM_addrx1 &&
         val.getForm() <= llvm::dwarf::DW_FORM_addrx4))
        return fetchFromDebugAddr(unit.debug_addr_base, val.getRawUValue());
    throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Form {} for {} is not supported",
        llvm::dwarf::FormEncodingString(val.getForm()).operator std::string_view(),
        llvm::dwarf::AttributeString(attr).operator std::string_view());
}

Chunk DWARFBlockInputFormat::parseEntries(UnitState & unit)
{
    const auto & header = getPort().getHeader();
    const auto & column_name_to_idx = getColumnNameToIdx();
    std::array<bool, COL_COUNT> need{};
    for (const std::string & name : header.getNames())
        need[column_name_to_idx.at(name)] = true;

    /// For parallel arrays, we nominate one of them to be responsible for populating the offsets vector.
    need[COL_ATTR_NAME] = need[COL_ATTR_NAME] || need[COL_ATTR_FORM] || need[COL_ATTR_INT] || need[COL_ATTR_STR];
    need[COL_ANCESTOR_TAGS] = need[COL_ANCESTOR_TAGS] || need[COL_ANCESTOR_OFFSETS];

    auto col_offset = ColumnVector<UInt64>::create();
    auto col_size = ColumnVector<UInt32>::create();
    auto col_tag = ColumnVector<UInt16>::create();
    auto col_ancestor_tags = ColumnVector<UInt16>::create();
    auto col_ancestor_dwarf_offsets = ColumnVector<UInt64>::create();
    auto col_ancestor_array_offsets = ColumnVector<UInt64>::create();
    auto col_name = ColumnString::create();
    auto col_linkage_name = ColumnString::create();
    ColumnLowCardinality::Index col_decl_file;
    auto col_decl_line = ColumnVector<UInt32>::create();
    auto col_ranges_start = ColumnVector<UInt64>::create();
    auto col_ranges_end = ColumnVector<UInt64>::create();
    auto col_ranges_offsets = ColumnVector<UInt64>::create();
    auto col_attr_name = ColumnVector<UInt16>::create();
    auto col_attr_form = ColumnVector<UInt16>::create();
    auto col_attr_int = ColumnVector<UInt64>::create();
    auto col_attr_str = ColumnLowCardinality::create(MutableColumnPtr(ColumnUnique<ColumnString>::create(ColumnString::create()->cloneResized(1), /*is_nullable*/ false)), MutableColumnPtr(ColumnVector<UInt16>::create()));
    auto col_attr_offsets = ColumnVector<UInt64>::create();
    size_t num_rows = 0;
    auto err = llvm::Error::success();

    while (num_rows < 65536)
    {
        ++num_rows;
        uint64_t die_offset = unit.offset;
        if (need[COL_OFFSET])
            col_offset->insertValue(die_offset);
        if (need[COL_ANCESTOR_TAGS])
        {
            for (size_t i = unit.stack.size() - 1; i != UINT64_MAX; --i)
            {
                col_ancestor_tags->insertValue(unit.stack[i].tag);
                if (need[COL_ANCESTOR_OFFSETS])
                    col_ancestor_dwarf_offsets->insertValue(unit.stack[i].offset);
            }
            col_ancestor_array_offsets->insertValue(col_ancestor_tags->size());
        }

        uint64_t abbrev_code = extractor->getULEB128(&unit.offset, &err);
        throwIfError(err, "DIE header");

        if (abbrev_code == 0)
        {
            if (need[COL_SIZE])
                col_size->insertValue(static_cast<UInt32>(unit.offset - die_offset));
            if (need[COL_TAG])
                col_tag->insertValue(0); // "null"

            if (need[COL_NAME]) col_name->insertDefault();
            if (need[COL_LINKAGE_NAME]) col_linkage_name->insertDefault();
            if (need[COL_DECL_FILE]) col_decl_file.insertPosition(0);
            if (need[COL_DECL_LINE]) col_decl_line->insertDefault();
            if (need[COL_RANGES]) col_ranges_offsets->insertValue(col_ranges_start->size());
            if (need[COL_ATTR_NAME]) col_attr_offsets->insertValue(col_attr_name->size());

            if (unit.stack.empty())
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Stack underflow");
            unit.stack.pop_back();
        }
        else
        {
            const llvm::DWARFAbbreviationDeclaration * abbrev = unit.abbrevs->getAbbreviationDeclaration(static_cast<uint32_t>(abbrev_code));
            if (abbrev == nullptr || abbrev_code > UINT32_MAX)
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Abbrev code in DIE header is out of bounds: {}, offset {}", abbrev_code, unit.offset);

            auto tag = abbrev->getTag();
            if (need[COL_TAG])
                col_tag->insertValue(tag);

            if (tag == llvm::dwarf::DW_TAG_compile_unit)
            {
                /// Pre-parse DW_AT_addr_base and DW_AT_rnglists_base because other attributes may
                /// rely on them. (Why couldn't DWARF just promise that these attributes must appear
                /// before any attributes that depend on them?)
                uint64_t offset = unit.offset;
                std::optional<llvm::DWARFFormValue> low_pc;
                for (auto attr : abbrev->attributes())
                {
                    if (attr.Attr == llvm::dwarf::DW_AT_addr_base ||
                        attr.Attr == llvm::dwarf::DW_AT_rnglists_base)
                    {
                        auto val = parseAttribute(attr, &offset, unit);
                        if (attr.Attr == llvm::dwarf::DW_AT_addr_base)
                            unit.debug_addr_base = val.getRawUValue();
                        else
                            unit.rnglists_base = val.getRawUValue();
                    }
                    else if (attr.Attr == llvm::dwarf::DW_AT_low_pc)
                        low_pc = parseAttribute(attr, &offset, unit);
                    else
                        skipAttribute(attr, &offset, unit);
                }
                /// May use addr_base.
                if (low_pc.has_value())
                    unit.base_address = parseAddress(llvm::dwarf::DW_AT_low_pc, *low_pc, unit);
            }

            bool need_name = need[COL_NAME];
            bool need_linkage_name = need[COL_LINKAGE_NAME];
            bool need_decl_file = need[COL_DECL_FILE];
            bool need_decl_line = need[COL_DECL_LINE];

            /// Address ranges can be encoded in the following ways:
            ///  * DW_AT_ranges - list of ranges; points to other sections, nontrivial to parse;
            ///                   DW_AT_low_pc may also be present, used as base address during parsing
            ///  * [DW_AT_low_pc, DW_AT_high_pc) if DW_AT_high_pc has form "address"
            ///  * [DW_AT_low_pc, DW_AT_low_pc + DW_AT_high_pc) if DW_AT_high_pc has form "constant"
            ///  * [DW_AT_low_pc, DW_AT_low_pc + 1) if DW_AT_high_pc is absent
            bool need_ranges = need[COL_RANGES];
            std::optional<UInt64> ranges;
            bool ranges_rnglistx = false; // `ranges` has an extra indirection through offsets table
            std::optional<UInt64> low_pc;
            std::optional<UInt64> high_pc;
            bool relative_high_pc = false;

            for (auto attr : abbrev->attributes())
            {
                auto val = parseAttribute(attr, &unit.offset, unit);

                if (need[COL_ATTR_NAME])
                    col_attr_name->insertValue(attr.Attr);
                /// Note that in case of DW_FORM_implicit_const val.getForm() is different from attr.Form.
                /// Not sure which one would be more useful in the attr_form column. Guessing attr.Form for now.
                if (need[COL_ATTR_FORM])
                    col_attr_form->insertValue(attr.Form);

                if (attr.Attr == llvm::dwarf::DW_AT_stmt_list && unit.filename_table == nullptr)
                {
                    /// We expect that this attribute appears before any attributes that point into the filename table.
                    auto offset = val.getAsSectionOffset();
                    if (offset.has_value())
                        parseFilenameTable(unit, offset.value());
                }

                switch (val.getForm()) // (may be different from attr.Form because of DW_FORM_indirect)
                {
                    /// A 64-bit value.
                    case llvm::dwarf::DW_FORM_data2:
                    case llvm::dwarf::DW_FORM_data4:
                    case llvm::dwarf::DW_FORM_data8:
                    case llvm::dwarf::DW_FORM_data1:
                    case llvm::dwarf::DW_FORM_sdata:
                    case llvm::dwarf::DW_FORM_udata:
                    case llvm::dwarf::DW_FORM_data16:
                    case llvm::dwarf::DW_FORM_flag:
                    case llvm::dwarf::DW_FORM_flag_present:
                    case llvm::dwarf::DW_FORM_loclistx: // points to .debug_loclists
                    case llvm::dwarf::DW_FORM_rnglistx: // points to .debug_rnglists
                    case llvm::dwarf::DW_FORM_sec_offset: // points to some other section, depending on attr.Attr
                    case llvm::dwarf::DW_FORM_implicit_const:
                        if (need[COL_ATTR_INT]) col_attr_int->insertValue(val.getRawUValue());

                        if (attr.Attr == llvm::dwarf::DW_AT_decl_line && std::exchange(need_decl_line, false))
                            col_decl_line->insertValue(static_cast<UInt32>(val.getRawUValue()));

                        if (attr.Attr == llvm::dwarf::DW_AT_high_pc)
                        {
                            high_pc = val.getRawUValue();
                            relative_high_pc = true;
                        }

                        if (attr.Attr == llvm::dwarf::DW_AT_ranges)
                        {
                            ranges = val.getRawUValue();
                            ranges_rnglistx = val.getForm() == llvm::dwarf::DW_FORM_rnglistx;
                        }

                        /// Some attribute values are indices into lookup tables that we can stringify usefully.
                        if ((attr.Attr == llvm::dwarf::DW_AT_decl_file || attr.Attr == llvm::dwarf::DW_AT_call_file) &&
                            val.getRawUValue() < unit.filename_table_size) // filename
                        {
                            UInt64 idx = val.getRawUValue() + 1;
                            if (attr.Attr == llvm::dwarf::DW_AT_decl_file && std::exchange(need_decl_file, false))
                                col_decl_file.insertPosition(idx);

                            if (need[COL_ATTR_STR])
                            {
                                auto data = unit.filename_table->getDataAt(idx);
                                col_attr_str->insertData(data.data, data.size);
                            }
                        }
                        else if (need[COL_ATTR_STR])
                        {
                            static constexpr std::string_view DW_LANG_ = "DW_LANG_";
                            static constexpr std::string_view DW_ATE_ = "DW_ATE_";
                            if (attr.Attr == llvm::dwarf::DW_AT_language) // programming language
                                append(col_attr_str, removePrefix(llvm::dwarf::LanguageString(static_cast<uint32_t>(val.getRawUValue())),
                                    DW_LANG_.size()));
                            else if (attr.Attr == llvm::dwarf::DW_AT_encoding) // primitive type
                                append(col_attr_str, removePrefix(llvm::dwarf::AttributeEncodingString(static_cast<uint32_t>(val.getRawUValue())),
                                    DW_ATE_.size()));
                            else
                                col_attr_str->insertDefault();
                        }
                        break;

                    /// An address, i.e. just a 64-bit value.
                    /// May have indirection to .debug_addr section.
                    case llvm::dwarf::DW_FORM_addr:
                    case llvm::dwarf::DW_FORM_addrx:
                    case llvm::dwarf::DW_FORM_addrx1:
                    case llvm::dwarf::DW_FORM_addrx2:
                    case llvm::dwarf::DW_FORM_addrx3:
                    case llvm::dwarf::DW_FORM_addrx4:
                    case llvm::dwarf::DW_FORM_GNU_addr_index:
                    case llvm::dwarf::DW_FORM_LLVM_addrx_offset:
                        if (need[COL_ATTR_INT]) col_attr_int->insertValue(val.getAsAddress().value_or(0));
                        if (need[COL_ATTR_STR]) col_attr_str->insertDefault();

                        if (need_ranges && (attr.Attr == llvm::dwarf::DW_AT_low_pc || attr.Attr == llvm::dwarf::DW_AT_high_pc))
                        {
                            UInt64 addr = parseAddress(attr.Attr, val, unit);
                            if (attr.Attr == llvm::dwarf::DW_AT_low_pc)
                                low_pc = addr;
                            else
                                high_pc = addr;
                        }

                        break;

                    /// A byte string.
                    case llvm::dwarf::DW_FORM_block2:
                    case llvm::dwarf::DW_FORM_block4:
                    case llvm::dwarf::DW_FORM_block:
                    case llvm::dwarf::DW_FORM_block1:
                    case llvm::dwarf::DW_FORM_exprloc: // DWARF expression
                    {
                        auto slice = val.getAsBlock().value_or(llvm::ArrayRef<uint8_t>());
                        if (need[COL_ATTR_STR]) col_attr_str->insertData(reinterpret_cast<const char *>(slice.data()), slice.size());
                        if (need[COL_ATTR_INT]) col_attr_int->insertDefault();
                        break;
                    }

                    /// A text string.
                    /// May have indirection to .debug_str or .debug_line_str.
                    case llvm::dwarf::DW_FORM_string:
                    case llvm::dwarf::DW_FORM_strp:
                    case llvm::dwarf::DW_FORM_strx:
                    case llvm::dwarf::DW_FORM_strp_sup:
                    case llvm::dwarf::DW_FORM_line_strp:
                    case llvm::dwarf::DW_FORM_strx1:
                    case llvm::dwarf::DW_FORM_strx2:
                    case llvm::dwarf::DW_FORM_strx3:
                    case llvm::dwarf::DW_FORM_strx4:
                    case llvm::dwarf::DW_FORM_GNU_str_index:
                    case llvm::dwarf::DW_FORM_GNU_strp_alt:
                    {
                        auto res = val.getAsCString();
                        if (auto e = res.takeError())
                            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF,
                                "Error parsing string attribute: {}", llvm::toString(std::move(e)));
                        size_t len = strlen(*res);

                        if (attr.Attr == llvm::dwarf::DW_AT_name)
                        {
                            if (std::exchange(need_name, false))
                                col_name->insertData(*res, len);
                            if (tag == llvm::dwarf::DW_TAG_compile_unit)
                                unit.unit_name = *res;
                        }
                        if (attr.Attr == llvm::dwarf::DW_AT_linkage_name && std::exchange(need_linkage_name, false))
                            col_linkage_name->insertData(*res, len);

                        if (need[COL_ATTR_STR]) col_attr_str->insertData(*res, len);
                        if (need[COL_ATTR_INT]) col_attr_int->insertDefault();
                        break;
                    }

                    /// Offset of another entry in .debug_info.
                    case llvm::dwarf::DW_FORM_ref_addr:
                    case llvm::dwarf::DW_FORM_ref1:
                    case llvm::dwarf::DW_FORM_ref2:
                    case llvm::dwarf::DW_FORM_ref4:
                    case llvm::dwarf::DW_FORM_ref8:
                    case llvm::dwarf::DW_FORM_ref_udata:
                    case llvm::dwarf::DW_FORM_ref_sup4:
                    case llvm::dwarf::DW_FORM_ref_sig8:
                    case llvm::dwarf::DW_FORM_ref_sup8:
                    case llvm::dwarf::DW_FORM_GNU_ref_alt:
                        // If the offset is relative to the current unit, we convert it to be relative to the .debug_info
                        // section start. This seems more convenient for the user (e.g. for JOINs), but it's
                        // also confusing to see e.g. DW_FORM_ref4 (unit-relative reference) next to an absolute offset.
                        if (need[COL_ATTR_INT])
                        {
                            uint64_t ref;
                            if (std::optional<uint64_t> offset = val.getAsRelativeReference())
                                ref = val.getUnit()->getOffset() + *offset;
                            else if (offset = val.getAsDebugInfoReference(); offset)
                                ref = *offset;
                            else
                                ref = 0;

                            col_attr_int->insertValue(ref);
                        }
                        if (need[COL_ATTR_STR]) col_attr_str->insertDefault();
                        break;

                    default:
                        if (need[COL_ATTR_INT]) col_attr_int->insertDefault();
                        if (need[COL_ATTR_STR]) col_attr_str->insertDefault();
                }
            }

            if (need[COL_SIZE])
                col_size->insertValue(static_cast<UInt32>(unit.offset - die_offset));
            if (need[COL_ATTR_NAME])
                col_attr_offsets->insertValue(col_attr_name->size());

            if (need_name) col_name->insertDefault();
            if (need_linkage_name) col_linkage_name->insertDefault();
            if (need_decl_file) col_decl_file.insertPosition(0);
            if (need_decl_line) col_decl_line->insertDefault();

            if (need_ranges)
            {
                if (ranges.has_value())
                    parseRanges(*ranges, ranges_rnglistx, unit, col_ranges_start, col_ranges_end);
                else if (low_pc.has_value())
                {
                    UInt64 high;
                    if (!high_pc.has_value())
                        high = *low_pc + 1;
                    else if (relative_high_pc)
                        high = *low_pc + *high_pc ;
                    else
                        high = *high_pc;
                    col_ranges_start->insertValue(*low_pc);
                    col_ranges_end->insertValue(high);
                }
            }
            if (need[COL_RANGES]) col_ranges_offsets->insertValue(col_ranges_start->size());

            if (abbrev->hasChildren())
                unit.stack.push_back(StackEntry{.offset = die_offset, .tag = tag});
        }

        if (unit.stack.empty())
        {
            if (!unit.eof())
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Unexpected end of DIE tree at offset {} != {}", unit.offset, unit.end_offset);
            break;
        }
    }

    ColumnPtr immutable_attr_offsets = std::move(col_attr_offsets);
    ColumnPtr immutable_ancestor_array_offsets = std::move(col_ancestor_array_offsets);

    Columns cols;
    for (const std::string & name : header.getNames())
    {
        switch (column_name_to_idx.at(name))
        {
            case COL_OFFSET:
                cols.push_back(std::exchange(col_offset, nullptr));
                break;
            case COL_SIZE:
                cols.push_back(std::exchange(col_size, nullptr));
                break;
            case COL_TAG:
                cols.push_back(ColumnLowCardinality::create(tag_dict_column, std::exchange(col_tag, nullptr), /*is_shared*/ true));
                break;
            case COL_UNIT_NAME:
            {
                auto dict = ColumnString::create();
                dict->insertDefault();
                dict->insertData(unit.unit_name.data(), unit.unit_name.size());
                auto index = ColumnVector<UInt8>::create();
                index->insert(1);
                auto indices = index->replicate({num_rows});
                cols.push_back(ColumnLowCardinality::create(ColumnUnique<ColumnString>::create(
                    std::move(dict), /*is_nullable*/ false), indices));
                break;
            }
            case COL_UNIT_OFFSET:
            {
                auto dict = ColumnVector<UInt64>::create();
                dict->insertDefault();
                dict->insertValue(unit.dwarf_unit->getOffset());
                auto index = ColumnVector<UInt8>::create();
                index->insert(1);
                auto indices = index->replicate({num_rows});
                cols.push_back(ColumnLowCardinality::create(ColumnUnique<ColumnVector<UInt64>>::create(
                    std::move(dict), /*is_nullable*/ false), indices));
                break;
            }
            case COL_ANCESTOR_TAGS:
                cols.push_back(ColumnArray::create(ColumnLowCardinality::create(
                    tag_dict_column, std::exchange(col_ancestor_tags, nullptr), /*is_shared*/ true), immutable_ancestor_array_offsets));
                break;
            case COL_ANCESTOR_OFFSETS:
                cols.push_back(ColumnArray::create(std::exchange(col_ancestor_dwarf_offsets, nullptr), immutable_ancestor_array_offsets));
                break;
            case COL_NAME:
                cols.push_back(std::exchange(col_name, nullptr));
                break;
            case COL_LINKAGE_NAME:
                cols.push_back(std::exchange(col_linkage_name, nullptr));
                break;
            case COL_DECL_FILE:
                cols.push_back(ColumnLowCardinality::create(unit.filename_table, col_decl_file.detachPositions(), /*is_shared*/ true));
                break;
            case COL_DECL_LINE:
                cols.push_back(std::exchange(col_decl_line, nullptr));
                break;
            case COL_RANGES:
                cols.push_back(ColumnArray::create(ColumnTuple::create(Columns{std::exchange(col_ranges_start, nullptr), std::exchange(col_ranges_end, nullptr)}), std::exchange(col_ranges_offsets, nullptr)));
                break;
            case COL_ATTR_NAME:
                cols.push_back(ColumnArray::create(ColumnLowCardinality::create(
                    attr_name_dict_column, std::exchange(col_attr_name, nullptr), /*is_shared*/ true), immutable_attr_offsets));
                break;
            case COL_ATTR_FORM:
                cols.push_back(ColumnArray::create(ColumnLowCardinality::create(
                    attr_form_dict_column, std::exchange(col_attr_form, nullptr), /*is_shared*/ true), immutable_attr_offsets));
                break;
            case COL_ATTR_INT:
                cols.push_back(ColumnArray::create(std::exchange(col_attr_int, nullptr), immutable_attr_offsets));
                break;
            case COL_ATTR_STR:
                cols.push_back(ColumnArray::create(std::exchange(col_attr_str, nullptr), immutable_attr_offsets));
                break;

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column index");
        }
    }
    return Chunk(std::move(cols), num_rows);
}

void DWARFBlockInputFormat::parseFilenameTable(UnitState & unit, uint64_t offset)
{
    if (!debug_line_extractor.has_value())
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "There are DW_AT_stmt_list but no .debug_line section");

    llvm::DWARFDebugLine::Prologue prologue;
    auto error = prologue.parse(*debug_line_extractor, &offset, /*RecoverableErrorHandler*/ [&](auto e)
        {
            if (++seen_debug_line_warnings < 10)
                LOG_INFO(getLogger("DWARF"), "Parsing error: {}", llvm::toString(std::move(e)));
        }, *dwarf_context, unit.dwarf_unit);

    if (error)
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Failed to parse .debug_line unit prologue: {}", llvm::toString(std::move(error)));

    auto col = ColumnString::create();
    col->insertDefault();
    /// DWARF v5 changed file indexes from 1-based to 0-based.
    if (prologue.getVersion() <= 4)
        append(col, "<invalid>");
    for (const auto & entry : prologue.FileNames)
    {
        auto val = entry.Name.getAsCString();
        const char * c_str;
        if (llvm::Error e = val.takeError())
        {
            c_str = "<error>";
            llvm::consumeError(std::move(e));
        }
        else
            c_str = *val;
        col->insertData(c_str, strlen(c_str));
    }
    unit.filename_table_size = col->size() - 1;
    unit.filename_table = ColumnUnique<ColumnString>::create(std::move(col), /*is_nullable*/ false);
}

uint64_t DWARFBlockInputFormat::fetchFromDebugAddr(uint64_t addr_base, uint64_t idx) const
{
    if (!debug_addr_section.has_value())
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Missing .debug_addr section.");
    if (addr_base == UINT64_MAX)
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Missing DW_AT_addr_base");
    uint64_t offset = addr_base + idx * 8;
    if (offset + 8 > debug_addr_section->size())
        throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, ".debug_addr offset out of bounds: {} vs {}.", offset, debug_addr_section->size());
    uint64_t res;
    memcpy(&res, debug_addr_section->data() + offset, 8);
    return res;
}

void DWARFBlockInputFormat::parseRanges(
    uint64_t offset, bool form_rnglistx, const UnitState & unit, const ColumnVector<UInt64>::MutablePtr & col_ranges_start,
    const ColumnVector<UInt64>::MutablePtr & col_ranges_end) const
{
    std::optional<llvm::object::SectionedAddress> base_addr;
    if (unit.base_address != UINT64_MAX)
        base_addr = llvm::object::SectionedAddress{.Address = unit.base_address};

    llvm::DWARFAddressRangesVector ranges;

    if (unit.dwarf_unit->getFormParams().Version < 5)
    {
        if (!debug_ranges_extractor.has_value())
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Missing .debug_ranges section");

        llvm::DWARFDebugRangeList list;
        auto err = list.extract(*debug_ranges_extractor, &offset);
        if (err)
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Error parsing .debug_ranges list: {}", llvm::toString(std::move(err)));

        ranges = list.getAbsoluteRanges(base_addr);
    }
    else
    {
        if (!debug_rnglists_extractor.has_value())
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Missing .debug_rnglists section");

        if (form_rnglistx)
        {
            /// Fetch offset from a table.
            if (unit.rnglists_base == UINT64_MAX)
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Missing DW_AT_rnglists_base");
            uint64_t entry_size = unit.dwarf_unit->getFormParams().getDwarfOffsetByteSize();
            uint64_t lists_offset = unit.rnglists_base + offset * entry_size;
            if (lists_offset + entry_size > debug_rnglists_extractor->size())
                throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "DW_FORM_rnglistx offset out of bounds: {} vs {}", lists_offset, debug_rnglists_extractor->size());

            offset = 0;
            memcpy(&offset, debug_rnglists_extractor->getData().data() + lists_offset, entry_size);

            offset += unit.rnglists_base;
        }

        llvm::DWARFDebugRnglist list;
        auto err = list.extract(*debug_rnglists_extractor, /*HeaderOffset, essentially unused*/ 0, &offset,
            ".debug_rnglists", "rnglists");
        if (err)
            throw Exception(ErrorCodes::CANNOT_PARSE_DWARF, "Error parsing .debug_rnglists list: {}", llvm::toString(std::move(err)));

        auto lookup_addr = [&](uint32_t idx) -> std::optional<llvm::object::SectionedAddress>
            {
                uint64_t addr = fetchFromDebugAddr(unit.debug_addr_base, idx);
                return llvm::object::SectionedAddress{.Address = addr};
            };
        ranges = list.getAbsoluteRanges(base_addr, /*AddressByteSize*/ 8, lookup_addr);
    }

    for (const llvm::DWARFAddressRange & r : ranges)
    {
        col_ranges_start->insertValue(r.LowPC);
        col_ranges_end->insertValue(r.HighPC);
    }
}

Chunk DWARFBlockInputFormat::read()
{
    initializeIfNeeded();

    std::unique_lock lock(mutex);
    bool ok = false;
    SCOPE_EXIT({
        if (!ok)
        {
            is_stopped = true;
            wake_up_threads.notify_all();
        }
    });

    while (true)
    {
        if (is_stopped)
            return {};
        if (background_exception)
            std::rethrow_exception(background_exception);

        if (!delivery_queue.empty())
        {
            Chunk chunk = std::move(delivery_queue.front().first);
            approx_bytes_read_for_chunk = delivery_queue.front().second;
            delivery_queue.pop_front();
            wake_up_threads.notify_one();
            ok = true;
            return chunk;
        }

        if (units_queue.empty() && units_in_progress == 0)
            return {};

        deliver_chunk.wait(lock);
    }
}

void DWARFBlockInputFormat::resetParser()
{
    stopThreads();

    pool.reset();
    background_exception = nullptr;
    is_stopped = false;
    units_queue.clear();
    delivery_queue.clear();
    units_in_progress = 0;
    elf.reset();
    extractor.reset();

    IInputFormat::resetParser();
}

DWARFSchemaReader::DWARFSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList DWARFSchemaReader::readSchema()
{
    return getHeaderForDWARF();
}

void registerDWARFSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "DWARF",
        [](ReadBuffer & buf, const FormatSettings &)
        {
            return std::make_shared<DWARFSchemaReader>(buf);
        }
    );
}

void registerInputFormatDWARF(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "DWARF",
        [](ReadBuffer & buf,
            const Block & sample,
            const FormatSettings & settings,
            const ReadSettings &,
            bool /* is_remote_fs */,
            size_t /* max_download_threads */,
            size_t max_parsing_threads)
        {
            return std::make_shared<DWARFBlockInputFormat>(
                buf,
                sample,
                settings,
                max_parsing_threads);
        });
    factory.markFormatSupportsSubsetOfColumns("DWARF");
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatDWARF(FormatFactory &)
{
}

void registerDWARFSchemaReader(FormatFactory &) {}
}

#endif
