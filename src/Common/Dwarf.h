#pragma once

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

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>


namespace DB
{

class Elf;

/**
 * DWARF record parser.
 *
 * We only implement enough DWARF functionality to convert from PC address
 * to file and line number information.
 *
 * This means (although they're not part of the public API of this class), we
 * can parse Debug Information Entries (DIEs), abbreviations, attributes (of
 * all forms), and we can interpret bytecode for the line number VM.
 *
 * We can interpret DWARF records of version 2, 3, or 4, although we don't
 * actually support many of the version 4 features (such as VLIW, multiple
 * operations per instruction)
 *
 * Note that the DWARF record parser does not allocate heap memory at all.
 * This is on purpose: you can use the parser from
 * memory-constrained situations (such as an exception handler for
 * std::out_of_memory)  If it weren't for this requirement, some things would
 * be much simpler: the Path class would be unnecessary and would be replaced
 * with a std::string; the list of file names in the line number VM would be
 * kept as a vector of strings instead of re-executing the program to look for
 * DW_LNE_define_file instructions, etc.
 */
class Dwarf final
{
    // Note that Dwarf uses (and returns) std::string_view a lot.
    // The std::string_view point within sections in the ELF file, and so will
    // be live for as long as the passed-in Elf is live.
public:
    /** Create a DWARF parser around an ELF file. */
    explicit Dwarf(const std::shared_ptr<Elf> & elf);

    /**
     * More than one location info may exist if current frame is an inline
     * function call.
     */
    static constexpr uint32_t kMaxInlineLocationInfoPerFrame = 10;

    /**
      * Represent a file path a s collection of three parts (base directory,
      * subdirectory, and file).
      */
    class Path
    {
    public:
        Path() = default;

        Path(std::string_view baseDir, std::string_view subDir, std::string_view file);

        std::string_view baseDir() const { return baseDir_; }
        std::string_view subDir() const { return subDir_; }
        std::string_view file() const { return file_; }

        size_t size() const;

        /**
        * Copy the Path to a buffer of size bufSize.
        *
        * toBuffer behaves like snprintf: It will always null-terminate the
        * buffer (so it will copy at most bufSize-1 bytes), and it will return
        * the number of bytes that would have been written if there had been
        * enough room, so, if toBuffer returns a value >= bufSize, the output
        * was truncated.
        */
        size_t toBuffer(char * buf, size_t bufSize) const;

        void toString(std::string & dest) const;
        std::string toString() const
        {
            std::string s;
            toString(s);
            return s;
        }

        // TODO(tudorb): Implement operator==, operator!=; not as easy as it
        // seems as the same path can be represented in multiple ways
    private:
        std::string_view baseDir_;
        std::string_view subDir_;
        std::string_view file_;
    };

    // Indicates inline function `name` is called  at `line@file`.
    struct CallLocation
    {
        Path file = {};
        uint64_t line = 0;
        std::string_view name;
    };

    enum class LocationInfoMode
    {
        // Don't resolve location info.
        DISABLED,
        // Perform CU lookup using .debug_aranges (might be incomplete).
        FAST,
        // Scan all CU in .debug_info (slow!) on .debug_aranges lookup failure.
        FULL,
        // Scan .debug_info (super slower, use with caution) for inline functions in
        // addition to FULL.
        FULL_WITH_INLINE,
    };

    struct LocationInfo
    {
        bool has_main_file = false;
        Path main_file;

        bool has_file_and_line = false;
        Path file;
        uint64_t line = 0;
    };

    /**
     * Frame information: symbol name and location.
     */
    struct SymbolizedFrame
    {
        bool found = false;
        uintptr_t addr = 0;
        // Mangled symbol name. Use `folly::demangle()` to demangle it.
        const char * name = nullptr;
        LocationInfo location;
        std::shared_ptr<const Elf> file;

        void clear() { *this = SymbolizedFrame(); }
    };

    /** Find the file and line number information corresponding to address.
      * The address must be physical - offset in object file without offset in virtual memory where the object is loaded.
      */
    bool findAddress(uintptr_t address, LocationInfo & info, LocationInfoMode mode, std::vector<SymbolizedFrame> & inline_frames) const;

private:
    static bool findDebugInfoOffset(uintptr_t address, std::string_view aranges, uint64_t & offset);

    void init();

    std::shared_ptr<const Elf> elf_;

    // DWARF section made up of chunks, each prefixed with a length header.
    // The length indicates whether the chunk is DWARF-32 or DWARF-64, which
    // guides interpretation of "section offset" records.
    // (yes, DWARF-32 and DWARF-64 sections may coexist in the same file)
    class Section
    {
    public:
        Section() : is64Bit_(false) {}

        explicit Section(std::string_view d);

        // Return next chunk, if any; the 4- or 12-byte length was already
        // parsed and isn't part of the chunk.
        bool next(std::string_view & chunk);

        // Is the current chunk 64 bit?
        bool is64Bit() const { return is64Bit_; }

    private:
        // Yes, 32- and 64- bit sections may coexist.  Yikes!
        bool is64Bit_;
        std::string_view data_;
    };

    // Abbreviation for a Debugging Information Entry.
    struct DIEAbbreviation
    {
        uint64_t code = 0;
        uint64_t tag = 0;
        bool has_children = false;

        std::string_view attributes;
    };

    // Debugging information entry to define a low-level representation of a
    // source program. Each debugging information entry consists of an identifying
    // tag and a series of attributes. An entry, or group of entries together,
    // provide a description of a corresponding entity in the source program.
    struct Die
    {
        bool is64Bit;
        // Offset from start to first attribute
        uint8_t attr_offset;
        // Offset within debug info.
        uint32_t offset;
        uint64_t code;
        DIEAbbreviation abbr;
    };

    struct AttributeSpec
    {
        uint64_t name = 0;
        uint64_t form = 0;

        explicit operator bool() const { return name != 0 || form != 0; }
    };

    struct Attribute
    {
        AttributeSpec spec;
        const Die & die;
        std::variant<uint64_t, std::string_view> attr_value;
    };

    struct CompilationUnit
    {
        bool is64Bit;
        uint8_t version;
        uint8_t addr_size;
        // Offset in .debug_info of this compilation unit.
        uint32_t offset;
        uint32_t size;
        // Offset in .debug_info for the first DIE in this compilation unit.
        uint32_t first_die;
        uint64_t abbrev_offset;
        // Only the CompilationUnit that contains the caller functions needs this cache.
        // Indexed by (abbr.code - 1) if (abbr.code - 1) < abbrCache.size();
        std::vector<DIEAbbreviation> abbr_cache;
    };

    static CompilationUnit getCompilationUnit(std::string_view info, uint64_t offset);

    /** cu must exist during the life cycle of created detail::Die. */
    Die getDieAtOffset(const CompilationUnit & cu, uint64_t offset) const;

    /**
     * Find the actual definition DIE instead of declaration for the given die.
     */
    Die findDefinitionDie(const CompilationUnit & cu, const Die & die) const;

    bool findLocation(
        uintptr_t address,
        LocationInfoMode mode,
        CompilationUnit & cu,
        LocationInfo & info,
        std::vector<SymbolizedFrame> & inline_frames) const;

    /**
     * Finds a subprogram debugging info entry that contains a given address among
     * children of given die. Depth first search.
     */
    void findSubProgramDieForAddress(
        const CompilationUnit & cu, const Die & die, uint64_t address, std::optional<uint64_t> base_addr_cu, Die & subprogram) const;

    // Interpreter for the line number bytecode VM
    class LineNumberVM
    {
    public:
        LineNumberVM(std::string_view data, std::string_view compilationDirectory);

        bool findAddress(uintptr_t target, Path & file, uint64_t & line);

        /** Gets full file name at given index including directory. */
        Path getFullFileName(uint64_t index) const
        {
            auto fn = getFileName(index);
            return Path({}, getIncludeDirectory(fn.directoryIndex), fn.relativeName);
        }

    private:
        void init();
        void reset();

        // Execute until we commit one new row to the line number matrix
        bool next(std::string_view & program);
        enum StepResult
        {
            CONTINUE, // Continue feeding opcodes
            COMMIT, // Commit new <address, file, line> tuple
            END, // End of sequence
        };
        // Execute one opcode
        StepResult step(std::string_view & program);

        struct FileName
        {
            std::string_view relativeName;
            // 0 = current compilation directory
            // otherwise, 1-based index in the list of include directories
            uint64_t directoryIndex;
        };
        // Read one FileName object, remove_prefix program
        static bool readFileName(std::string_view & program, FileName & fn);

        // Get file name at given index; may be in the initial table
        // (fileNames_) or defined using DW_LNE_define_file (and we reexecute
        // enough of the program to find it, if so)
        FileName getFileName(uint64_t index) const;

        // Get include directory at given index
        std::string_view getIncludeDirectory(uint64_t index) const;

        // Execute opcodes until finding a DW_LNE_define_file and return true;
        // return file at the end.
        bool nextDefineFile(std::string_view & program, FileName & fn) const;

        // Initialization
        bool is64Bit_;
        std::string_view data_;
        std::string_view compilationDirectory_;

        // Header
        uint16_t version_;
        uint8_t minLength_;
        bool defaultIsStmt_;
        int8_t lineBase_;
        uint8_t lineRange_;
        uint8_t opcodeBase_;
        const uint8_t * standardOpcodeLengths_;

        std::string_view includeDirectories_;
        size_t includeDirectoryCount_;

        std::string_view fileNames_;
        size_t fileNameCount_;

        // State machine registers
        uint64_t address_;
        uint64_t file_;
        uint64_t line_;
        uint64_t column_;
        bool isStmt_;
        bool basicBlock_;
        bool endSequence_;
        bool prologueEnd_;
        bool epilogueBegin_;
        uint64_t isa_;
        uint64_t discriminator_;
    };

    /**
     * Finds inlined subroutine DIEs and their caller lines that contains a given
     * address among children of given die. Depth first search.
     */
    void findInlinedSubroutineDieForAddress(
        const CompilationUnit & cu,
        const Die & die,
        const LineNumberVM & line_vm,
        uint64_t address,
        std::optional<uint64_t> base_addr_cu,
        std::vector<CallLocation> & locations,
        size_t max_size) const;

    // Read an abbreviation from a std::string_view, return true if at end; remove_prefix section
    static bool readAbbreviation(std::string_view & section, DIEAbbreviation & abbr);

    static void readCompilationUnitAbbrs(std::string_view abbrev, CompilationUnit & cu);

    /**
     * Iterates over all children of a debugging info entry, calling the given
     * callable for each. Iteration is stopped early if any of the calls return
     * false. Returns the offset of next DIE after iterations.
     */
    size_t forEachChild(const CompilationUnit & cu, const Die & die, std::function<bool(const Die & die)> f) const;

    // Get abbreviation corresponding to a code, in the chunk starting at
    // offset in the .debug_abbrev section
    DIEAbbreviation getAbbreviation(uint64_t code, uint64_t offset) const;

    /**
     * Iterates over all attributes of a debugging info entry, calling the given
     * callable for each. If all attributes are visited, then return the offset of
     * next DIE, or else iteration is stopped early and return size_t(-1) if any
     * of the calls return false.
     */
    size_t forEachAttribute(const CompilationUnit & cu, const Die & die, std::function<bool(const Attribute & die)> f) const;

    Attribute readAttribute(const Die & die, AttributeSpec spec, std::string_view & info) const;

    // Read one attribute <name, form> pair, remove_prefix sp; returns <0, 0> at end.
    static AttributeSpec readAttributeSpec(std::string_view & sp);

    // Read one attribute value, remove_prefix sp
    using AttributeValue = std::variant<uint64_t, std::string_view>;
    AttributeValue readAttributeValue(std::string_view & sp, uint64_t form, bool is64Bit) const;

    // Get an ELF section by name, return true if found
    bool getSection(const char * name, std::string_view * section) const;

    // Get a string from the .debug_str section
    std::string_view getStringFromStringSection(uint64_t offset) const;

    template <class T>
    std::optional<T> getAttribute(const CompilationUnit & cu, const Die & die, uint64_t attr_name) const
    {
        std::optional<T> result;
        forEachAttribute(cu, die, [&](const Attribute & attr)
        {
            if (attr.spec.name == attr_name)
            {
                result = std::get<T>(attr.attr_value);
                return false;
            }
            return true;
        });
        return result;
    }

    // Check if the given address is in the range list at the given offset in .debug_ranges.
    bool isAddrInRangeList(uint64_t address, std::optional<uint64_t> base_addr, size_t offset, uint8_t addr_size) const;

    // Finds the Compilation Unit starting at offset.
    static CompilationUnit findCompilationUnit(std::string_view info, uint64_t targetOffset);

    std::string_view info_; // .debug_info
    std::string_view abbrev_; // .debug_abbrev
    std::string_view aranges_; // .debug_aranges
    std::string_view line_; // .debug_line
    std::string_view strings_; // .debug_str
    std::string_view ranges_; // .debug_ranges
};

}

#endif
