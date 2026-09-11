#pragma once

#include <cstdint>
#include <elf.h>


/** Deciphered ELF (Executable and Linkable Format) data structures used by the dynamic loader.
  *
  * The system header <elf.h> declares the canonical structures (Elf64_Ehdr, Elf64_Phdr, ...), but their
  * field names are terse Unix abbreviations from the 1990s. To keep the loader readable we mirror the exact
  * binary layout with fully spelled out names, and static_assert that the layout still matches <elf.h>.
  *
  * Glossary of the abbreviations you will meet in the ELF world:
  *
  *   ELF        - Executable and Linkable Format (the object/executable/shared-library file format on Linux).
  *   Ehdr       - ELF header (the very first structure in the file).
  *   Phdr       - program header (describes a segment to be mapped into memory at run time).
  *   Shdr       - section header (describes a section; used at link time, not needed to run).
  *   Dyn        - an entry of the ".dynamic" section (a tag/value pair driving dynamic linking).
  *   Sym        - a symbol table entry.
  *   Rel/Rela   - a relocation entry ("Rela" = relocation with an explicit addend).
  *   Relr       - a relative-relocation entry using the compact "RELR" encoding.
  *   vaddr      - virtual address.
  *   paddr      - physical address (unused on Linux user space).
  *   filesz     - size of the segment as stored in the file.
  *   memsz      - size of the segment once mapped in memory (>= filesz; the extra tail is zero-filled ".bss").
  *   offset     - byte offset inside the file.
  *   phoff      - program header table offset (inside the file).
  *   shoff      - section header table offset.
  *   phnum      - number of program headers; phentsize - size of one program header.
  *   PT_*       - program header Type (LOAD, DYNAMIC, INTERP, TLS, GNU_RELRO, ...).
  *   PF_*       - program header Flags (Read/Write/eXecute permissions).
  *   DT_*       - Dynamic Tag (NEEDED, STRTAB, SYMTAB, RELA, INIT, ...).
  *   PLT        - Procedure Linkage Table (trampolines for lazy function binding).
  *   GOT        - Global Offset Table (a table of resolved addresses filled in by relocations).
  *   RELRO      - RELocations Read-Only (a region made read-only after relocations, to harden the GOT).
  *   TLS        - Thread-Local Storage.
  *   DTV        - Dynamic Thread Vector (per-thread array of pointers to each module's TLS block).
  *   TCB        - Thread Control Block (the per-thread structure the hardware thread pointer points at).
  *   IFUNC      - Indirect FUNCtion (a symbol whose real address is computed by calling a resolver).
  *   SONAME     - Shared Object Name (the canonical name a library advertises itself under).
  *   RPATH      - Run-time search PATH (a deprecated hard-coded library search path).
  *   RUNPATH    - the modern replacement for RPATH.
  *   STB_*      - Symbol Table Binding (LOCAL, GLOBAL, WEAK).
  *   STT_*      - Symbol Table Type (OBJECT, FUNC, GNU_IFUNC, TLS, ...).
  *   SHN_UNDEF  - Section header index meaning "undefined" (an imported symbol).
  */

namespace DB::DynamicLinker
{

/// The identification bytes at the start of every ELF file (the "e_ident" array).
enum ElfIdentificationIndex
{
    ELF_CLASS_INDEX = 4,      /// 32-bit vs 64-bit.
    ELF_DATA_INDEX = 5,       /// Little-endian vs big-endian.
    ELF_VERSION_INDEX = 6,
    ELF_OS_ABI_INDEX = 7,
};

/// The first structure in the file. Mirrors Elf64_Ehdr.
struct ElfHeader
{
    unsigned char identification[16];    /// e_ident: magic bytes "\x7fELF", class, data encoding, version.
    uint16_t type;                       /// e_type: ET_DYN for a shared library or position-independent executable.
    uint16_t machine;                    /// e_machine: target architecture (EM_X86_64, EM_AARCH64, ...).
    uint32_t version;                    /// e_version.
    uint64_t entry_point;                /// e_entry: entry point virtual address (unused for a library).
    uint64_t program_header_offset;      /// e_phoff.
    uint64_t section_header_offset;      /// e_shoff.
    uint32_t flags;                      /// e_flags.
    uint16_t elf_header_size;            /// e_ehsize.
    uint16_t program_header_entry_size;  /// e_phentsize.
    uint16_t program_header_count;       /// e_phnum.
    uint16_t section_header_entry_size;  /// e_shentsize.
    uint16_t section_header_count;       /// e_shnum.
    uint16_t section_names_index;        /// e_shstrndx.
};
static_assert(sizeof(ElfHeader) == sizeof(Elf64_Ehdr));

/// Program header type (the "p_type" field): what a segment is used for.
enum class SegmentType : uint32_t
{
    Load = PT_LOAD,               /// A chunk of the file to map into memory.
    Dynamic = PT_DYNAMIC,         /// The ".dynamic" section, driving dynamic linking.
    Interpreter = PT_INTERP,      /// Path of the requested interpreter (the real ld.so); ignored by us.
    Note = PT_NOTE,
    ThreadLocalStorage = PT_TLS,  /// The template for thread-local variables.
    ProgramHeaders = PT_PHDR,
    GNURelocationsReadOnly = PT_GNU_RELRO,
    GNUStack = PT_GNU_STACK,
    GNUExceptionFrame = PT_GNU_EH_FRAME,
};

/// Program header permission flags (the "p_flags" field).
enum SegmentFlags : uint32_t
{
    SEGMENT_EXECUTABLE = PF_X,
    SEGMENT_WRITABLE = PF_W,
    SEGMENT_READABLE = PF_R,
};

/// One program header: describes a segment. Mirrors Elf64_Phdr.
struct ProgramHeader
{
    uint32_t type;                  /// p_type: see SegmentType.
    uint32_t flags;                 /// p_flags: see SegmentFlags.
    uint64_t file_offset;           /// p_offset: where the segment's bytes start in the file.
    uint64_t virtual_address;       /// p_vaddr: link-time virtual address (relative for ET_DYN).
    uint64_t physical_address;      /// p_paddr: unused on Linux user space.
    uint64_t file_size;             /// p_filesz: bytes present in the file.
    uint64_t memory_size;           /// p_memsz: bytes occupied in memory (tail beyond file_size is zeroed).
    uint64_t alignment;             /// p_align.
};
static_assert(sizeof(ProgramHeader) == sizeof(Elf64_Phdr));

/// Dynamic table tag (the "d_tag" field of a ".dynamic" entry).
enum class DynamicTag : int64_t
{
    Null = DT_NULL,                             /// Terminates the array.
    Needed = DT_NEEDED,                         /// String-table offset of a required library name.
    PLTRelocationsSize = DT_PLTRELSZ,           /// Total size of the PLT relocation table.
    PLTGlobalOffsetTable = DT_PLTGOT,
    Hash = DT_HASH,                             /// System-V style symbol hash table.
    StringTable = DT_STRTAB,                    /// Address of the string table.
    SymbolTable = DT_SYMTAB,                    /// Address of the symbol table.
    RelocationsWithAddend = DT_RELA,            /// Address of the RELA relocation table.
    RelocationsWithAddendSize = DT_RELASZ,
    RelocationsWithAddendEntrySize = DT_RELAENT,
    StringTableSize = DT_STRSZ,
    SymbolEntrySize = DT_SYMENT,
    Init = DT_INIT,                             /// Address of the (legacy) initialization function.
    Fini = DT_FINI,                             /// Address of the (legacy) finalization function.
    SharedObjectName = DT_SONAME,
    RunPathDeprecated = DT_RPATH,
    Symbolic = DT_SYMBOLIC,
    Relocations = DT_REL,                       /// REL table (no addend) - rare on 64-bit.
    RelocationsSize = DT_RELSZ,
    RelocationsEntrySize = DT_RELENT,
    PLTRelocationType = DT_PLTREL,              /// Whether the PLT relocations are REL or RELA.
    Debug = DT_DEBUG,
    TextRelocations = DT_TEXTREL,               /// Relocations touch read-only text (needs it writable first).
    PLTRelocations = DT_JMPREL,                 /// Address of the PLT relocation table.
    BindNow = DT_BIND_NOW,
    InitArray = DT_INIT_ARRAY,                  /// Address of an array of initialization functions.
    FiniArray = DT_FINI_ARRAY,                  /// Address of an array of finalization functions.
    InitArraySize = DT_INIT_ARRAYSZ,
    FiniArraySize = DT_FINI_ARRAYSZ,
    RunPath = DT_RUNPATH,                       /// Library search path.
    Flags = DT_FLAGS,
    PreInitArray = DT_PREINIT_ARRAY,            /// Pre-initializers (main executable only).
    PreInitArraySize = DT_PREINIT_ARRAYSZ,
    RelativeRelocations = DT_RELR,              /// Compact RELR relative-relocation table.
    RelativeRelocationsSize = DT_RELRSZ,
    RelativeRelocationsEntrySize = DT_RELRENT,
    Flags1 = DT_FLAGS_1,
    GNUHash = DT_GNU_HASH,                      /// GNU-style symbol hash table (faster; present in modern libs).
    VersionSymbol = DT_VERSYM,                  /// Per-symbol version indices.
    VersionDefinitions = DT_VERDEF,             /// Version definitions this object provides.
    VersionDefinitionsCount = DT_VERDEFNUM,
    VersionNeeded = DT_VERNEED,                 /// Versions this object requires from its dependencies.
    VersionNeededCount = DT_VERNEEDNUM,
};

/// One ".dynamic" entry. Mirrors Elf64_Dyn.
struct DynamicEntry
{
    int64_t tag;                    /// d_tag: see DynamicTag.
    uint64_t value;                 /// d_un.d_val / d_un.d_ptr: either a value or an (unrelocated) address.
};
static_assert(sizeof(DynamicEntry) == sizeof(Elf64_Dyn));

/// A symbol table entry. Mirrors Elf64_Sym.
struct Symbol
{
    uint32_t name_offset;           /// st_name: offset into the string table.
    uint8_t info;                   /// st_info: binding (high nibble) and type (low nibble).
    uint8_t visibility;             /// st_other: symbol visibility (DEFAULT, HIDDEN, ...).
    uint16_t section_index;         /// st_shndx: defining section, or SHN_UNDEF for an import.
    uint64_t value;                 /// st_value: link-time address (or TLS offset for a TLS symbol).
    uint64_t size;                  /// st_size.

    /// st_info packs binding and type together, hence the ELF64_ST_BIND / ELF64_ST_TYPE macros.
    uint8_t binding() const { return info >> 4; }     /// STB_LOCAL / STB_GLOBAL / STB_WEAK.
    uint8_t type() const { return info & 0xf; }       /// STT_FUNC / STT_OBJECT / STT_GNU_IFUNC / STT_TLS ...
    bool isDefined() const { return section_index != SHN_UNDEF; }
    bool isWeak() const { return binding() == STB_WEAK; }
    bool isIndirectFunction() const { return type() == STT_GNU_IFUNC; }
    bool isThreadLocal() const { return type() == STT_TLS; }
};
static_assert(sizeof(Symbol) == sizeof(Elf64_Sym));

/// A relocation entry with an explicit addend. Mirrors Elf64_Rela.
struct RelocationWithAddend
{
    uint64_t offset;                /// r_offset: address (relative to load base) to patch.
    uint64_t info;                  /// r_info: packs the symbol index (high 32 bits) and type (low 32 bits).
    int64_t addend;                 /// r_addend: constant added to the computed value.

    uint32_t symbolIndex() const { return static_cast<uint32_t>(info >> 32); }   /// ELF64_R_SYM.
    uint32_t type() const { return static_cast<uint32_t>(info & 0xffffffff); }   /// ELF64_R_TYPE.
};
static_assert(sizeof(RelocationWithAddend) == sizeof(Elf64_Rela));

/// A relocation entry without an addend. Mirrors Elf64_Rel (only used when an object emits REL, not RELA).
struct Relocation
{
    uint64_t offset;
    uint64_t info;

    uint32_t symbolIndex() const { return static_cast<uint32_t>(info >> 32); }
    uint32_t type() const { return static_cast<uint32_t>(info & 0xffffffff); }
};
static_assert(sizeof(Relocation) == sizeof(Elf64_Rel));

/// Symbol version definition/need structures (mirrors the Elf64_Ver* family from <elf.h>).
using VersionDefinition = Elf64_Verdef;         /// vd_*: a version this object defines.
using VersionDefinitionAux = Elf64_Verdaux;     /// vda_*: the name(s) of a defined version.
using VersionNeed = Elf64_Verneed;              /// vn_*: versions required from one dependency.
using VersionNeedAux = Elf64_Vernaux;           /// vna_*: one required version and its index.

/// Special version indices (the low 15 bits of a DT_VERSYM entry; bit 15 is the "hidden" flag).
enum VersionIndex : uint16_t
{
    VERSION_LOCAL = 0,          /// Local symbol, not exported.
    VERSION_GLOBAL = 1,         /// Global, unversioned ("base") symbol.
    VERSION_HIDDEN_FLAG = 0x8000,
    VERSION_INDEX_MASK = 0x7fff,
};

/// The argument passed to __tls_get_addr for the general-dynamic TLS model.
/// (glibc calls this "tls_index"; both fields are module-relative.)
struct ThreadLocalStorageIndex
{
    uint64_t module_id;             /// Which loaded module the variable belongs to (1-based).
    uint64_t offset;                /// Byte offset of the variable inside that module's TLS block.
};

/** Relocation type codes for the host architecture, deciphered.
  *
  * The numeric values come straight from <elf.h> (the R_X86_64_* / R_AARCH64_* macros). They tell the loader
  * how to compute the value written at a relocation's target. "S" = symbol address, "A" = addend,
  * "B" = load bias (the difference between where we mapped the object and its link-time base).
  */
namespace RelocationType
{
#if defined(__x86_64__)
    constexpr uint32_t Direct64 = R_X86_64_64;                          /// Write S + A.
    constexpr uint32_t Copy = R_X86_64_COPY;                            /// Copy the symbol's bytes (data import).
    constexpr uint32_t GlobalData = R_X86_64_GLOB_DAT;                  /// Write S into a GOT slot.
    constexpr uint32_t JumpSlot = R_X86_64_JUMP_SLOT;                   /// Write S into a PLT/GOT slot.
    constexpr uint32_t Relative = R_X86_64_RELATIVE;                    /// Write B + A (rebase a pointer).
    constexpr uint32_t TLSModuleID = R_X86_64_DTPMOD64;                 /// Module id of the symbol's owner.
    constexpr uint32_t TLSModuleOffset = R_X86_64_DTPOFF64;            /// Offset within that module's TLS block.
    constexpr uint32_t TLSThreadPointerOffset = R_X86_64_TPOFF64;      /// Static (initial-exec) TLS - unsupported.
    constexpr uint32_t TLSDescriptor = R_X86_64_TLSDESC;               /// TLS descriptor model - unsupported.
    constexpr uint32_t IndirectRelative = R_X86_64_IRELATIVE;          /// Call resolver at B + A, write result.
#elif defined(__aarch64__)
    constexpr uint32_t Direct64 = R_AARCH64_ABS64;
    constexpr uint32_t Copy = R_AARCH64_COPY;
    constexpr uint32_t GlobalData = R_AARCH64_GLOB_DAT;
    constexpr uint32_t JumpSlot = R_AARCH64_JUMP_SLOT;
    constexpr uint32_t Relative = R_AARCH64_RELATIVE;
    constexpr uint32_t TLSModuleID = R_AARCH64_TLS_DTPMOD;
    constexpr uint32_t TLSModuleOffset = R_AARCH64_TLS_DTPREL;
    constexpr uint32_t TLSThreadPointerOffset = R_AARCH64_TLS_TPREL;
    constexpr uint32_t TLSDescriptor = R_AARCH64_TLSDESC;
    constexpr uint32_t IndirectRelative = R_AARCH64_IRELATIVE;
#else
#error "The dynamic loader supports only x86-64 and AArch64."
#endif
}

/// The e_machine value we expect for the host architecture.
#if defined(__x86_64__)
constexpr uint16_t HOST_ELF_MACHINE = EM_X86_64;
#elif defined(__aarch64__)
constexpr uint16_t HOST_ELF_MACHINE = EM_AARCH64;
#endif

}
