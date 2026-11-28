#include "config.h"

#if defined(__ELF__) && !defined(OS_FREEBSD) && !USE_FUZZING_MODE

#include <Common/CurrentThread.h>
#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/HashTable/Hash.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <base/scope_guard.h>

#include <zlib.h>
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;
using namespace std::string_view_literals;

namespace
{

enum class Codec : uint32_t { None = 0, Zlib = 1, Zstd = 2, Unknown = 3 };

struct Writer
{
    std::string data;

    template <typename T> void put(T value) { data.append(reinterpret_cast<const char *>(&value), sizeof(value)); }
    void bytes(std::initializer_list<uint8_t> values) { for (uint8_t value : values) put(value); }
    void str(std::string_view s) { data.append(s); data.push_back('\0'); }
    template <typename T> void patch(size_t offset, T value) { memcpy(data.data() + offset, &value, sizeof(value)); }
};

std::string compress(std::string_view raw, Codec codec)
{
    if (codec == Codec::None)
        return std::string(raw);

    std::string payload;
    if (codec == Codec::Zlib || codec == Codec::Unknown)
    {
        uLongf size = compressBound(raw.size());
        payload.resize(size);
        compress2(reinterpret_cast<Bytef *>(payload.data()), &size, reinterpret_cast<const Bytef *>(raw.data()), raw.size(), 6);
        payload.resize(size);
    }
    else
    {
        payload.resize(ZSTD_compressBound(raw.size()));
        payload.resize(ZSTD_compress(payload.data(), payload.size(), raw.data(), raw.size(), 3));
    }

    Writer w;
    w.put(static_cast<uint32_t>(codec));
    w.put<uint32_t>(0);
    w.put<uint64_t>(raw.size());
    w.put<uint64_t>(1);
    return w.data + payload;
}

struct TestSection
{
    std::string name;
    std::string bytes;
    bool compressed = false;
    uint64_t extra_size = 0;
};

TestSection section(std::string name, std::string_view raw, Codec codec)
{
    return {std::move(name), compress(raw, codec), codec != Codec::None};
}

std::string makeElf(std::vector<TestSection> sections)
{
    std::string image(sizeof(ElfHeader) + sizeof(ElfProgramHeader), '\0');
    std::string names(1, '\0');
    std::vector<ElfSectionHeader> headers(1);

    sections.push_back({".shstrtab", ""});
    for (const auto & s : sections)
    {
        ElfSectionHeader & header = headers.emplace_back();
        header.name = static_cast<uint32_t>(names.size());
        header.type = SectionHeaderType::PROGBITS;
        header.flags = s.compressed ? static_cast<uint64_t>(SectionHeaderFlag::COMPRESSED) : 0;
        header.offset = image.size();
        header.size = s.bytes.size() + s.extra_size;
        names += s.name + '\0';
        image += s.bytes;
    }
    headers.back().type = SectionHeaderType::STRTAB;
    headers.back().size = names.size();
    image += names;

    ElfHeader header{};
    memcpy(header.ident, "\x7F" "ELF", 4);
    header.phoff = sizeof(ElfHeader);
    header.phnum = 1;
    header.shoff = image.size();
    header.shnum = static_cast<uint16_t>(headers.size());
    header.shstrndx = static_cast<uint16_t>(headers.size() - 1);
    memcpy(image.data(), &header, sizeof(header));
    image.append(reinterpret_cast<const char *>(headers.data()), headers.size() * sizeof(ElfSectionHeader));
    return image;
}

/// A DWARF 4 compile unit "test.cpp" covering [0x1000, 0x1100) whose line program maps 0x1000 to line 42.
std::vector<TestSection> dwarfSections(Codec codec)
{
    Writer abbrev;
    /// 1: DW_TAG_compile_unit, no children, (name, string), (stmt_list, sec_offset), (low_pc, addr), (high_pc, addr).
    abbrev.bytes({1, 0x11, 0, 0x03, 0x08, 0x10, 0x17, 0x11, 0x01, 0x12, 0x01, 0, 0, 0});

    Writer info;
    info.put<uint32_t>(0);
    info.put<uint16_t>(4);
    info.put<uint32_t>(0);
    info.bytes({8, 1});
    info.str("test.cpp");
    info.put<uint32_t>(0);
    info.put<uint64_t>(0x1000);
    info.put<uint64_t>(0x1100);
    info.patch<uint32_t>(0, static_cast<uint32_t>(info.data.size() - 4));

    Writer line;
    line.put<uint32_t>(0);
    line.put<uint16_t>(4);
    line.put<uint32_t>(0);
    line.bytes({1, 1, 1});
    line.put<int8_t>(-5);
    line.bytes({14, 13, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 1, 0});
    line.str("test.cpp");
    line.bytes({0, 0, 0, 0});
    line.patch<uint32_t>(6, static_cast<uint32_t>(line.data.size() - 10));
    /// DW_LNE_set_address 0x1000, DW_LNS_advance_line 41, DW_LNS_copy, DW_LNS_advance_pc 0x100, DW_LNE_end_sequence.
    line.bytes({0, 9, 2});
    line.put<uint64_t>(0x1000);
    line.bytes({3, 41, 1, 2, 0x80, 0x02, 0, 1, 1});
    line.patch<uint32_t>(0, static_cast<uint32_t>(line.data.size() - 4));

    return {section(".debug_abbrev", abbrev.data, codec), section(".debug_info", info.data, codec),
        section(".debug_line", line.data, codec), section(".debug_str", "unused\0"sv, codec)};
}

bool findLine42(const std::string & image)
{
    Dwarf dwarf(std::make_shared<Elf>(image.data(), image.size(), "<test>"));
    Dwarf::LocationInfo location;
    VectorWithMemoryTracking<Dwarf::SymbolizedFrame> frames;
    if (!dwarf.findAddress(0x1010, location, Dwarf::LocationInfoMode::FAST, frames))
        return false;
    return location.file.toString().ends_with("test.cpp") && location.line == 42;
}

std::string sampleData(size_t size)
{
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>('a' + intHash64(i) % 26);
    return data;
}

}

TEST(DwarfCompressedSections, FindAddress)
{
    for (auto codec : {Codec::None, Codec::Zlib, Codec::Zstd})
        EXPECT_TRUE(findLine42(makeElf(dwarfSections(codec)))) << "codec " << static_cast<uint32_t>(codec);
}

TEST(DwarfCompressedSections, UnreadableSectionDisablesDwarf)
{
    auto unknown_type = dwarfSections(Codec::None);
    unknown_type.push_back(section(".debug_str_offsets", std::string(64, 'x'), Codec::Unknown));

    auto truncated = dwarfSections(Codec::None);
    truncated.push_back({".debug_str_offsets", std::string(64, 'x'), false, 1ULL << 40});

    for (const auto & sections : {unknown_type, truncated})
    {
        bool found = true;
        EXPECT_NO_THROW(found = findLine42(makeElf(sections)));
        EXPECT_FALSE(found);
    }
}

TEST(DwarfCompressedSections, GetSectionsData)
{
    const std::string raw = sampleData(1 << 20);
    const auto image = makeElf({section(".zlib", raw, Codec::Zlib), section(".zstd", raw, Codec::Zstd), section(".plain", raw, Codec::None)});
    static constexpr const char * names[] = {".zlib", ".zstd", ".plain", ".absent"};
    Elf elf(image.data(), image.size(), "<test>");

    std::optional<std::vector<std::string_view>> results[3];
    std::thread thread([&] { results[0] = elf.getSectionsData(names); });
    results[1] = elf.getSectionsData(names);
    thread.join();
    results[2] = elf.getSectionsData(names);

    for (const auto & result : results)
    {
        ASSERT_TRUE(result);
        EXPECT_EQ(*result, (std::vector<std::string_view>{raw, raw, raw, {}}));
        for (size_t i = 0; i < 3; ++i)
            EXPECT_EQ((*result)[i].data(), (*results[0])[i].data());
    }
    EXPECT_EQ((*results[0])[2].data(), elf.findSectionByName(".plain")->begin());
}

TEST(DwarfCompressedSections, UnreadableSections)
{
    const std::string raw = sampleData(64 << 10);
    static constexpr const char * names[] = {".section"};

    auto with_size = [&](uint64_t size)
    {
        TestSection result = section(".section", raw, Codec::Zstd);
        memcpy(result.bytes.data() + 8, &size, sizeof(size));
        return result;
    };
    TestSection corrupt = section(".section", raw, Codec::Zlib);
    corrupt.bytes[corrupt.bytes.size() / 2] ^= 0x55;

    const std::vector<TestSection> cases = {
        section(".section", raw, Codec::Unknown),
        with_size(1ULL << 62),
        with_size(raw.size() + 1),
        {".section", std::string(8, 'x'), true},
        corrupt,
        {".section", raw, false, 1ULL << 40},
    };
    for (const auto & test_section : cases)
    {
        const auto image = makeElf({test_section});
        Elf elf(image.data(), image.size(), "<test>");
        EXPECT_FALSE(elf.getSectionsData(names));
        EXPECT_FALSE(elf.getSectionsData(names));
    }

    {
        const auto image = makeElf({section(".section", raw, Codec::Zstd)});
        Elf elf(image.data(), image.size(), "<test>");
        const Int64 hard_limit = total_memory_tracker.getHardLimit();
        {
            SCOPE_EXIT(total_memory_tracker.setHardLimit(hard_limit));
            /// Earlier tests in the same process can leave the tracked amount negative.
            total_memory_tracker.setHardLimit(std::max(total_memory_tracker.get(), total_memory_tracker.getRSS()) + 1024);
            /// As while an exception is formatted: the memory tracker does not throw.
            LockMemoryExceptionInThread lock(VariableContext::Global);
            EXPECT_FALSE(elf.getSectionsData(names));
        }
        EXPECT_FALSE(elf.getSectionsData(names));
        EXPECT_TRUE(Elf(image.data(), image.size(), "<test>").getSectionsData(names));
    }

    const auto image = makeElf({section(".valid", raw, Codec::Zstd), corrupt});
    static constexpr const char * both[] = {".valid", ".section"};
    static constexpr const char * valid[] = {".valid"};
    Elf elf(image.data(), image.size(), "<test>");
    EXPECT_FALSE(elf.getSectionsData(both));
    EXPECT_FALSE(elf.getSectionsData(valid));
    EXPECT_TRUE(Elf(image.data(), image.size(), "<test>").getSectionsData(valid));
}

TEST(DwarfCompressedSections, NotChargedToReader)
{
    const std::string raw = sampleData(1 << 20);
    const auto image = makeElf({section(".section", raw, Codec::Zstd)});
    static constexpr const char * names[] = {".section"};
    Elf elf(image.data(), image.size(), "<test>");

    /// The sections outlive whoever reads them first, so that thread is not charged for them.
    Int64 charged = 0;
    std::thread([&]
    {
        ThreadStatus thread_status;
        CurrentThread::flushUntrackedMemory();
        const Int64 before = CurrentThread::get().memory_tracker.get();
        EXPECT_TRUE(elf.getSectionsData(names));
        CurrentThread::flushUntrackedMemory();
        charged = CurrentThread::get().memory_tracker.get() - before;
    }).join();
    EXPECT_LT(charged, static_cast<Int64>(raw.size() / 16));
}

#endif
