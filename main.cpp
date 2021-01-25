/// clang++ -nostdinc -nostdinc++ -nostdlib -nodefaultlibs -static -g -O3 main.cpp && ./a.out 1 2 3
/// clang++ -nostdinc -nostdinc++ -nostdlib -nodefaultlibs -shared -fPIC -O3 -g main.cpp && ./a.out 1 2 3

#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))

#define __NR_write 1
#define __NR_exit_group 231

#define STDERR_FILENO 2


/// For auxv
#define AT_NULL         0
#define AT_IGNORE       1
#define AT_EXECFD       2
#define AT_PHDR         3
#define AT_PHENT        4
#define AT_PHNUM        5
#define AT_PAGESZ       6
#define AT_BASE         7
#define AT_FLAGS        8
#define AT_ENTRY        9
#define AT_NOTELF       10
#define AT_UID          11
#define AT_EUID         12
#define AT_GID          13
#define AT_EGID         14
#define AT_CLKTCK       17
#define AT_PLATFORM     15
#define AT_HWCAP        16
#define AT_FPUCW        18
#define AT_DCACHEBSIZE  19
#define AT_ICACHEBSIZE  20
#define AT_UCACHEBSIZE  21
#define AT_IGNOREPPC    22
#define AT_SECURE       23
#define AT_BASE_PLATFORM 24
#define AT_RANDOM       25
#define AT_HWCAP2       26
#define AT_EXECFN       31
#define AT_SYSINFO      32
#define AT_SYSINFO_EHDR 33
#define AT_L1I_CACHESHAPE       34
#define AT_L1D_CACHESHAPE       35
#define AT_L2_CACHESHAPE        36
#define AT_L3_CACHESHAPE        37
#define AT_L1I_CACHESIZE        40
#define AT_L1I_CACHEGEOMETRY    41
#define AT_L1D_CACHESIZE        42
#define AT_L1D_CACHEGEOMETRY    43
#define AT_L2_CACHESIZE         44
#define AT_L2_CACHEGEOMETRY     45
#define AT_L3_CACHESIZE         46
#define AT_L3_CACHEGEOMETRY     47
#define AT_MINSIGSTKSZ          51


using int32_t = int;
using uint32_t = unsigned int;
using int64_t = long;
using uint64_t = unsigned long;
using ssize_t = long;
using intptr_t = long;
using uintptr_t = unsigned long;
using size_t = unsigned long;


struct ProgramHeader
{
    enum Type : uint32_t
    {
        NULL = 0,    /// Program header table entry unused
        LOAD = 1,    /// Loadable program segment
        DYNAMIC = 2, /// Dynamic linking information
        INTERP = 3,  /// Program interpreter
        NOTE = 4,    /// Auxiliary information
        SHLIB = 5,   /// Reserved
        PHDR = 6,    /// Entry for header table itself
        TLS = 7,     /// Thread-local storage segment
        NUM = 8,     /// Number of defined types
        GNU_EH_FRAME = 0x6474e550, /// .eh_frame_hdr segment
        GNU_STACK = 0x6474e551,    /// Indicates stack executability
        GNU_RELRO = 0x6474e552,    /// Read-only after relocation
    };

    struct Flags
    {
        uint32_t data;

        bool readable() const { return data & 4; }
        bool writeable() const { return data & 2; }
        bool executable() const { return data & 1; }
    };

    Type type;
    Flags flags;
    uint64_t offset;
    char * virtual_address;
    char * physical_address;
    size_t size_in_file;
    size_t size_in_memory;
    size_t alignment;
};


struct DynamicTableEntry
{
    enum Tag : uint64_t
    {
        NULL = 0, /// Marks end of dynamic section
        NEEDED = 1, /// Name of needed library
        PLTRELSZ = 2, /// Size in bytes of PLT relocs
        PLTGOT = 3, /// Processor defined value
        HASH = 4, /// Address of symbol hash table
        STRTAB = 5, /// Address of string table
        SYMTAB = 6, /// Address of symbol table
        RELA = 7, /// Address of Rela relocs
        RELASZ = 8, /// Total size of Rela relocs
        RELAENT = 9, /// Size of one Rela reloc
        STRSZ = 10, /// Size of string table
        SYMENT = 11, /// Size of one symbol table entry
        INIT = 12, /// Address of init function
        FINI = 13, /// Address of termination function
        SONAME = 14, /// Name of shared object
        RPATH = 15, /// Library search path (deprecated)
        SYMBOLIC = 16, /// Start symbol search here
        REL = 17, /// Address of Rel relocs
        RELSZ = 18, /// Total size of Rel relocs
        RELENT = 19, /// Size of one Rel reloc
        PLTREL = 20, /// Type of reloc in PLT
        DEBUG = 21, /// For debugging; unspecified
        TEXTREL = 22, /// Reloc might modify .text
        JMPREL = 23, /// Address of PLT relocs
        BIND_NOW = 24, /// Process relocations of object
        INIT_ARRAY = 25, /// Array with addresses of init fct
        FINI_ARRAY = 26, /// Array with addresses of fini fct
        INIT_ARRAYSZ = 27, /// Size in bytes of DT_INIT_ARRAY
        FINI_ARRAYSZ = 28, /// Size in bytes of DT_FINI_ARRAY
        RUNPATH = 29, /// Library search path
        FLAGS = 30, /// Flags for the object being loaded
        ENCODING = 32, /// Start of encoded range
        PREINIT_ARRAY = 32, /// Array with addresses of preinit fct
        PREINIT_ARRAYSZ = 33, /// size in bytes of DT_PREINIT_ARRAY

        /* DT_* entries which fall between DT_VALRNGHI & DT_VALRNGLO use the
           Dyn.d_un.d_val field of the Elf*_Dyn structure.  This follows Sun's
           approach.
        */
        VALRNGLO = 0x6ffffd00,

        GNU_PRELINKED = 0x6ffffdf5, /// Prelinking timestamp
        GNU_CONFLICTSZ = 0x6ffffdf6, /// Size of conflict section
        GNU_LIBLISTSZ = 0x6ffffdf7, /// Size of library list
        CHECKSUM = 0x6ffffdf8,
        PLTPADSZ = 0x6ffffdf9,
        MOVEENT = 0x6ffffdfa,
        MOVESZ = 0x6ffffdfb,
        FEATURE_1 = 0x6ffffdfc, /// Feature selection (DTF_*).
        POSFLAG_1 = 0x6ffffdfd,    /* Flags for DT_* entries, effecting the following DT_* entry.  */
        SYMINSZ = 0x6ffffdfe, /// Size of syminfo table (in bytes)
        SYMINENT = 0x6ffffdff, /// Entry size of syminfo

        VALRNGHI = 0x6ffffdff,

        /* DT_* entries which fall between DT_ADDRRNGHI & DT_ADDRRNGLO use the
            Dyn.d_un.d_ptr field of the Elf*_Dyn structure.
            If any adjustment is made to the ELF object after it has been
            built these entries will need to be adjusted.
        */
        ADDRRNGLO = 0x6ffffe00,

        GNU_HASH = 0x6ffffef5, /// GNU-style hash table.
        TLSDESC_PLT = 0x6ffffef6,
        TLSDESC_GOT = 0x6ffffef7,
        GNU_CONFLICT = 0x6ffffef8, /// Start of conflict section
        GNU_LIBLIST = 0x6ffffef9, /// Library list
        CONFIG = 0x6ffffefa, /// Configuration information.
        DEPAUDIT = 0x6ffffefb, /// Dependency auditing.
        AUDIT = 0x6ffffefc, /// Object auditing.
        PLTPAD = 0x6ffffefd, /// PLT padding.
        MOVETAB = 0x6ffffefe, /// Move table.
        SYMINFO = 0x6ffffeff, /// Syminfo table.

        ADDRRNGHI = 0x6ffffeff,

        /* The versioning entry types.  The next are defined as part of the
            GNU extension.
        */
        VERSYM = 0x6ffffff0,

        RELACOUNT = 0x6ffffff9,
        RELCOUNT = 0x6ffffffa,
    };

    Tag tag;
    union
    {
        uint64_t value;
        char * ptr;
    };
};


/// Relocations relative to the base address. The meaning - simply add the base address.
struct Relocation
{
    char * address;
    uint64_t type;

    void process(uintptr_t base_address)
    {
        size_t * corrected_address = reinterpret_cast<size_t *>(address + base_address);
        *corrected_address += base_address;

    }
};

/// Add the base address and another operand ("addend").
struct RelocationWithAddend
{
    char * address;
    uint64_t type;
    int64_t addend;

    void process(uintptr_t base_address)
    {
        size_t * corrected_address = reinterpret_cast<size_t *>(address + base_address);
        *corrected_address += base_address + addend;
    }
};



/// These symbols have hidden linkage to avoid them being called through PLT.
/// It allows to use it before dynamic linking being performed.
namespace
{

NO_INLINE long internal_syscall_asm(long x0 = 0, long x1 = 0, long x2 = 0, long x3 = 0, long x4 = 0, long x5 = 0, long x6 = 0)
{
    long ret;
    __asm__ __volatile__ (R"(
        movq %[x0],%%rax;
        movq %[x1],%%rdi;
        movq %[x2],%%rsi;
        movq %[x3],%%rdx;
        movq %[x4],%%r10;
        movq %[x5],%%r8;
        movq %[x6],%%r9;
        syscall;
        movq %%rax,%[ret]
    )"
    : [ret]"=g"(ret)
    : [x0]"g"(x0), [x1]"g"(x1), [x2]"g"(x2), [x3]"g"(x3), [x4]"g"(x4), [x5]"g"(x5), [x6]"g"(x6)
    : "memory");
    return ret;
}

template <typename... Ts>
ALWAYS_INLINE long internal_syscall(Ts... args)
{
    return internal_syscall_asm((long)(args)...);
}


[[noreturn]] void _exit(int code)
{
    internal_syscall(__NR_exit_group, code);
    __builtin_unreachable();
}

ssize_t write(int fd, const void * buf, size_t count)
{
    return internal_syscall(__NR_write, static_cast<long>(fd), buf, count);
}

size_t strlen(const char * s)
{
    const char * end = s;
    while (*end)
        ++end;
    return end - s;
}

void print_string(const char * message)
{
    write(STDERR_FILENO, message, strlen(message));
}

void print_number(size_t num)
{
    char buf[20];
    char * pos = &buf[20];

    do
    {
        --pos;
        *pos = '0' + (num % 10);
        num /= 10;
    } while (num);

    write(STDERR_FILENO, pos, &buf[20] - pos);
}

void ALWAYS_INLINE assert(bool x)
{
    if (!x)
        __builtin_trap();
}

template <typename T>
T getauxval(uint64_t type, uint64_t * auxv)
{
    for (; *auxv; auxv += 2)
        if (auxv[0] == type)
            return *reinterpret_cast<T*>(&auxv[1]);
    return {};
}

}


extern "C"
{

int main(int argc, char ** argv, char ** envp)
{
    print_string("Hello, world!\n");
    return 0;
}


[[noreturn]] void _start()
{
    intptr_t * stack = static_cast<intptr_t *>(__builtin_frame_address(0));

    size_t argc = stack[1];
    char ** argv = reinterpret_cast<char **>(&stack[2]);
    assert(argv[argc] == nullptr);

    char ** envp = &argv[argc + 1];
    size_t envc = 0;
    for (size_t i = 0; envp[i]; ++i)
        ++envc;

    assert(envp[envc] == nullptr);

    uint64_t * auxv = reinterpret_cast<uint64_t *>(&envp[envc + 1]);
    size_t auxc = 0;
    for (size_t i = 0; auxv[i]; i += 2)
        ++auxc;

    print_number(argc);

    print_string("\n");

    for (size_t i = 0; i < argc; ++i)
    {
        print_string(argv[i]);
        print_string("\n");
    }

    print_string("\n");

    for (size_t i = 0; envp[i]; ++i)
    {
        print_string(envp[i]);
        print_string("\n");
    }

    print_string("\n");
    print_number(auxc);

    /// Check some auxv
    size_t page_size = getauxval<size_t>(AT_PAGESZ, auxv);

    print_string("\n");
    print_number(page_size);

    print_string("\n");

    size_t program_header_entry_size = getauxval<size_t>(AT_PHENT, auxv);
    assert(program_header_entry_size == sizeof(ProgramHeader));

    size_t num_program_headers = getauxval<size_t>(AT_PHNUM, auxv);
    print_number(num_program_headers);

    print_string("\n");

    ProgramHeader * program_headers = getauxval<ProgramHeader *>(AT_PHDR, auxv);

    uintptr_t base_address = getauxval<uintptr_t>(AT_BASE, auxv);
    if (!base_address)
        base_address = reinterpret_cast<uintptr_t>(program_headers) & (-page_size);

    for (size_t i = 0; i < num_program_headers; ++i)
    {
        print_number(program_headers[i].type);
        print_string(": ");
        print_number(program_headers[i].flags.data);
        print_string("\n");

        if (program_headers[i].type == ProgramHeader::DYNAMIC)
        {
            DynamicTableEntry * dynamic_table_entries = reinterpret_cast<DynamicTableEntry *>(
                base_address + program_headers[i].virtual_address);

            const DynamicTableEntry * rel = nullptr;
            const DynamicTableEntry * relsz = nullptr;
            const DynamicTableEntry * rela = nullptr;
            const DynamicTableEntry * relasz = nullptr;

            for (const auto * it = dynamic_table_entries; it->tag != DynamicTableEntry::NULL; ++it)
            {
                print_number(it->tag);
                print_string("\n");

                if (it->tag == DynamicTableEntry::REL)
                    rel = it;
                else if (it->tag == DynamicTableEntry::RELSZ)
                    relsz = it;
                else if (it->tag == DynamicTableEntry::RELA)
                    rela = it;
                else if (it->tag == DynamicTableEntry::RELASZ)
                    relasz = it;
            }

            /// Perform simple relocations

            if (rel && relsz)
            {
                Relocation * entries = reinterpret_cast<Relocation *>(base_address + rel->ptr);
                for (size_t j = 0; j < relsz->value; ++j)
                    entries[j].process(base_address);
            }

            if (rela && relasz)
            {
                RelocationWithAddend * entries = reinterpret_cast<RelocationWithAddend *>(base_address + rela->ptr);
                for (size_t j = 0; j < relasz->value; ++j)
                    entries[j].process(base_address);
            }
        }
    }

    print_string("\nRunning main...\n");
    _exit(main(argc, argv, envp));
}

}
