#include <common/phdr_cache.h>
#include <common/getThreadId.h>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>

#include <Poco/Environment.h>

#include <optional>
#include <string_view>

#include <signal.h>
#include <time.h>
#include <elf.h>
#include <link.h>

#include <gtest/gtest.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_MANIPULATE_SIGSET;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_CREATE_TIMER;
}


/// Auxiliary values are the values provided by Linux kernel that are needed for program
/// to run and to introspect something about itself and the system.
/// It can be imagined as a special hidden part of program arguments.

/// The typical contents of auxv can be introspected as following:
/// LD_SHOW_AUXV=1 /usr/bin/true

/// The description is at 'man getauxval'.

size_t getAuxiliaryValue(size_t type)
{
    /// The table with auxiliary values (also known as 'auxv')
    /// is placed by Linux kernel (or, subsequently, by dynamic loader) after 'envp' on program load.

    size_t environ_size = 0;
    while (__environ[environ_size])
        ++environ_size;

    const size_t * auxv = reinterpret_cast<const size_t *>(__environ + environ_size + 1);

    for (size_t i = 0; auxv[i]; i += 2)
        if (auxv[i] == type)
            return auxv[i + 1];

    return {};
}


template <typename Res, typename Src>
static const Res * shift(const Src * ptr, size_t bytes)
{
    return reinterpret_cast<const Res *>(reinterpret_cast<const char *>(ptr) + bytes);
}


/// Virtual dynamic shared object, also known as 'vdso'.
/// It is a small dynamic library that is provided by Linux kernel to every program
/// (along with a separate data page that is also mapped by Linux kernel at program startup)
/// and contains helpers to implement some functions (notably 'clock_gettime' and 'getcpu') avoiding system calls.

/// The description is at 'man vdso'
/// You can dump this library as a file with https://github.com/mattkeenan/dump-vdso/blob/master/dump-vdso
/// And introspect as following:
/// readelf -a vdso.elf
/// readelf --debug-dump=frames vdso.elf

/// The source code of vdso is located here:
/// https://github.com/torvalds/linux/blob/master/arch/x86/entry/vdso/vclock_gettime.c
/// https://github.com/torvalds/linux/blob/master/arch/x86/include/asm/vdso/gettimeofday.h
/// https://github.com/torvalds/linux/blob/5bfc75d92efd494db37f5c4c173d3639d4772966/include/vdso/datapage.h

void * getSymbolFromVirtualDynamicSharedObject(const char * version_name, const char * symbol_name)
{
    using Ehdr = Elf64_Ehdr;
    using Phdr = Elf64_Phdr;
    using Sym = Elf64_Sym;
    using Verdef = Elf64_Verdef;
    using Verdaux = Elf64_Verdaux;

    /// One of the auxv contains pointer to ELF header of the 'vdso' library loaded into memory.
    /// It allows us to locate "program headers" and iterate over them
    /// And then we are iterating over every symbol in the library
    /// looking for our needed symbol.

    const Ehdr * elf_header = reinterpret_cast<Ehdr *>(getAuxiliaryValue(AT_SYSINFO_EHDR));
    if (!elf_header)
        return nullptr;

    const Phdr * program_headers = shift<Phdr>(elf_header, elf_header->e_phoff);

    //std::cerr << program_headers << "\n";

    /// At what address the library is loaded.
    std::optional<size_t> base;

    /// Description of the dynamic library.
    const size_t * dynv = nullptr;

    for (size_t program_header_idx = 0; program_header_idx < elf_header->e_phnum; ++program_header_idx)
    {
        const Phdr * program_header = shift<Phdr>(program_headers, program_header_idx * elf_header->e_phentsize);

        if (program_header->p_type == PT_LOAD)
            base = reinterpret_cast<size_t>(elf_header) + program_header->p_offset - program_header->p_vaddr;
        else if (program_header->p_type == PT_DYNAMIC)
            dynv = shift<size_t>(elf_header, program_header->p_offset);
    }

    if (!dynv || !base)
        return nullptr;

    //std::cerr << *base << ", " << dynv << "\n";

    const char * strings = nullptr;
    const Sym * syms = nullptr;
    const Elf_Symndx * hash_table = nullptr;
    const uint16_t * versym = nullptr;
    const Verdef * verdef = nullptr;

    for (size_t i = 0; dynv[i]; i += 2)
    {
        const void * value_ptr = reinterpret_cast<void *>(*base + dynv[i + 1]);

        switch (dynv[i])
        {
            case DT_STRTAB: strings = reinterpret_cast<const char *>(value_ptr); break;
            case DT_SYMTAB: syms = reinterpret_cast<const Sym *>(value_ptr); break;
            case DT_HASH: hash_table = reinterpret_cast<const Elf_Symndx *>(value_ptr); break;
            case DT_VERSYM: versym = reinterpret_cast<const uint16_t *>(value_ptr); break;
            case DT_VERDEF: verdef = reinterpret_cast<const Verdef *>(value_ptr); break;
        }
    }

    if (!strings || !syms || !hash_table)
        return nullptr;

    if (!verdef)
        versym = nullptr;

    //std::cerr << "!\n";

    /// Despite the fact that we are looking into hashtable,
    /// we just iterate over it instead of doing hash lookup - for simplicity.

    for (size_t i = 0; i < hash_table[1]; ++i)
    {
        uint8_t symbol_type = syms[i].st_info % 16;
        uint8_t symbol_binding = syms[i].st_info / 16;

        if (symbol_type != STT_NOTYPE && symbol_type != STT_OBJECT && symbol_type != STT_FUNC && symbol_type != STT_COMMON)
            continue;

        if (symbol_binding != STB_GLOBAL && symbol_binding != STB_WEAK && symbol_binding != STB_GNU_UNIQUE)
            continue;

        if (!syms[i].st_shndx)
            continue;

        //std::cerr << (strings + syms[i].st_name) << "\n";

        if (std::string_view(symbol_name) != std::string_view(strings + syms[i].st_name))
            continue;

        //std::cerr << "!!\n";

        if (versym)
        {
            bool found = false;
            while (true)
            {
                //std::cerr << ".";

                if (!(verdef->vd_flags & VER_FLG_BASE) && (verdef->vd_ndx & 0x7FFF) == (versym[i] & 0x7FFF))
                {
                    found = true;
                    break;
                }
                if (verdef->vd_next == 0)
                {
                    break;
                }
                verdef = shift<Verdef>(verdef, verdef->vd_next);
            }

            if (!found)
                continue;

            const Verdaux * version_aux = shift<Verdaux>(verdef, verdef->vd_aux);

            //std::cerr << "'" << (strings + version_aux->vda_name) << "'" << "\n";

            if (std::string_view(version_name) != std::string_view(strings + version_aux->vda_name))
                continue;
        }

        //std::cerr << "!!!\n";

        return reinterpret_cast<void *>(*base + syms[i].st_value);
    }

    return 0;
}


using ClockGetTime = int (clockid_t, timespec *);
ClockGetTime * clock_gettime_from_vdso = nullptr;

static void initializeClockGettime()
{
    clock_gettime_from_vdso = reinterpret_cast<ClockGetTime *>(
        getSymbolFromVirtualDynamicSharedObject("LINUX_2.6", "__vdso_clock_gettime"));
}

static uint64_t nanoseconds()
{
    struct timespec ts;
    clock_gettime_from_vdso(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}


std::atomic<size_t> counter{};

static void signalHandler(int /*sig*/, siginfo_t * /*info*/, void * /*context*/)
{
    //(void)write(2, ".", 1);
    StackTrace();
    ++counter; // += StackTrace().getSize();
}

/// A loop with clock_gettime.
/// A signal by timer will be received 1000 times a second.
/// Inside the signal handler we will unwind the stack.
/// And we will check if asynchronoud unwinding works well within 'clock_gettime'.
/// It may not be the case if 'vdso' has invalid 'unwind tables'.
/// Note that 'vdso' is mapped by Linux kernel (and depend on kernel version).
void loop(double seconds)
{
    updatePHDRCache();
    initializeClockGettime();

    static constexpr auto TIMER_PRECISION = 1000000000;
    static constexpr auto period = 1000000;
    static constexpr auto pause_signal = SIGUSR1;

    timer_t timer_id = nullptr;

    struct sigaction sa{};
    sa.sa_sigaction = signalHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;

    if (sigemptyset(&sa.sa_mask))
        throwFromErrno("Failed to clean signal mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaddset(&sa.sa_mask, pause_signal))
        throwFromErrno("Failed to add signal to mask for query profiler", ErrorCodes::CANNOT_MANIPULATE_SIGSET);

    if (sigaction(pause_signal, &sa, nullptr))
        throwFromErrno("Failed to setup signal handler for query profiler", ErrorCodes::CANNOT_SET_SIGNAL_HANDLER);

    struct sigevent sev {};
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev.sigev_signo = pause_signal;

    sev._sigev_un._tid = getThreadId();

    if (timer_create(CLOCK_MONOTONIC, &sev, &timer_id))
        throwFromErrno("Failed to create thread timer", ErrorCodes::CANNOT_CREATE_TIMER);

    struct timespec interval{.tv_sec = period / TIMER_PRECISION, .tv_nsec = period % TIMER_PRECISION};
    struct timespec offset{.tv_sec = 0, .tv_nsec = 1};

    struct itimerspec timer_spec = {.it_interval = interval, .it_value = offset};
    if (timer_settime(timer_id, 0, &timer_spec, nullptr))
        throwFromErrno("Failed to set thread timer period", ErrorCodes::CANNOT_SET_TIMER_PERIOD);

    uint64_t prev_time = nanoseconds();
    size_t num_calls = 0;
    while (nanoseconds() - prev_time < seconds * 1e9)
        ++num_calls;

    std::cerr << "num_calls: " << num_calls << "\n";
}

}


TEST(AsynchronousUnwinding, Test)
{
    using namespace DB;

    std::cerr << "OS version is " << Poco::Environment::osVersion() << "\n";
    loop(10);
    std::cerr << counter << "\n";
}
