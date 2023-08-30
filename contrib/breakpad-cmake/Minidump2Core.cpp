// Copyright 2009 Google LLC
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Converts a minidump file to a core file which gdb can read.
// Large parts lifted from the userspace core dumper:
//   http://code.google.com/p/google-coredumper/

#include <filesystem>
#ifdef HAVE_CONFIG_H
#    include <config.h> // Must come first
#endif

#include <elf.h>
#include <errno.h>
#include <limits.h>
#include <link.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/user.h>

#include <map>
#include <string>
#include <vector>

#include "common/linux/memory_mapped_file.h"
#include "common/minidump_type_helper.h"
#include "common/path_helper.h"
#include "common/scoped_ptr.h"
#include "common/using_std_string.h"
#include "google_breakpad/common/breakpad_types.h"
#include "google_breakpad/common/minidump_format.h"
#include "third_party/lss/linux_syscall_support.h"
#include "tools/linux/md2core/minidump_memory_range.h"

#if ULONG_MAX == 0xffffffffffffffff
#    define ELF_CLASS ELFCLASS64
#else
#    define ELF_CLASS ELFCLASS32
#endif
#define Ehdr ElfW(Ehdr)
#define Phdr ElfW(Phdr)
#define Shdr ElfW(Shdr)
#define Nhdr ElfW(Nhdr)
#define auxv_t ElfW(auxv_t)


#if defined(__x86_64__)
#    define ELF_ARCH EM_X86_64
#elif defined(__i386__)
#    define ELF_ARCH EM_386
#elif defined(__arm__)
#    define ELF_ARCH EM_ARM
#elif defined(__mips__)
#    define ELF_ARCH EM_MIPS
#elif defined(__aarch64__)
#    define ELF_ARCH EM_AARCH64
#elif defined(__riscv)
#    define ELF_ARCH EM_RISCV
#endif

#if defined(__arm__)
// GLibc/ARM and Android/ARM both use 'user_regs' for the structure type
// containing core registers, while they use 'user_regs_struct' on other
// architectures. This file-local typedef simplifies the source code.
typedef user_regs user_regs_struct;
#elif defined(__mips__) || defined(__riscv)
// This file-local typedef simplifies the source code.
typedef gregset_t user_regs_struct;
#endif

using google_breakpad::MDTypeHelper;
using google_breakpad::MemoryMappedFile;
using google_breakpad::MinidumpMemoryRange;

typedef MDTypeHelper<sizeof(ElfW(Addr))>::MDRawDebug MDRawDebug;
typedef MDTypeHelper<sizeof(ElfW(Addr))>::MDRawLinkMap MDRawLinkMap;

static const MDRVA kInvalidMDRVA = static_cast<MDRVA>(-1);

struct Options
{
    string minidump_path;
    bool verbose;
    int out_fd;
    bool use_filename;
    bool inc_guid;
    string so_basedir;
};

static void Usage(int argc, const char * argv[])
{
    fprintf(
        stderr,
        "Usage: %s [options] <minidump file>\n"
        "\n"
        "Convert a minidump file into a core file (often for use by gdb).\n"
        "\n"
        "The shared library list will by default have filenames as the runtime expects.\n"
        "There are many flags to control the output names though to make it easier to\n"
        "integrate with your debug environment (e.g. gdb).\n"
        " Default:    /lib64/libpthread.so.0\n"
        " -f:         /lib64/libpthread-2.19.so\n"
        " -i:         /lib64/<module id>-libpthread.so.0\n"
        " -f -i:      /lib64/<module id>-libpthread-2.19.so\n"
        " -S /foo/:   /foo/libpthread.so.0\n"
        "\n"
        "Options:\n"
        "  -v         Enable verbose output\n"
        "  -o <file>  Write coredump to specified file (otherwise use stdout).\n"
        "  -f         Use the filename rather than the soname in the sharedlib list.\n"
        "             The soname is what the runtime system uses, but the filename is\n"
        "             how it's stored on disk.\n"
        "  -i         Prefix sharedlib names with ID (when available).  This makes it\n"
        "             easier to have a single directory full of symbols.\n"
        "  -S <dir>   Set soname base directory.  This will force all debug/symbol\n"
        "             lookups to be done in this directory rather than the filesystem\n"
        "             layout as it exists in the crashing image.  This path should end\n"
        "             with a slash if it's a directory.  e.g. /var/lib/breakpad/\n"
        "",
        google_breakpad::BaseName(argv[0]).c_str());
}

static void SetupOptions(int argc, const char * argv[], Options * options)
{
    extern int optind;
    int ch;
    const char * output_file = NULL;

    // Initialize the options struct as needed.
    options->verbose = false;
    options->use_filename = false;
    options->inc_guid = false;

    while ((ch = getopt(argc, (char * const *)argv, "fhio:S:v")) != -1)
    {
        switch (ch)
        {
            case 'h':
                Usage(argc, argv);
                exit(0);
            case '?':
                Usage(argc, argv);
                exit(1);

            case 'f':
                options->use_filename = true;
                break;
            case 'i':
                options->inc_guid = true;
                break;
            case 'o':
                output_file = optarg;
                break;
            case 'S':
                options->so_basedir = optarg;
                break;
            case 'v':
                options->verbose = true;
                break;
        }
    }

    if ((argc - optind) != 1)
    {
        fprintf(stderr, "%s: Missing minidump file\n", argv[0]);
        Usage(argc, argv);
        exit(1);
    }

    if (output_file == NULL || !strcmp(output_file, "-"))
    {
        options->out_fd = STDOUT_FILENO;
    }
    else
    {
        options->out_fd = open(output_file, O_WRONLY | O_CREAT | O_TRUNC, 0664);
        if (options->out_fd == -1)
        {
            fprintf(stderr, "%s: could not open output %s: %s\n", argv[0], output_file, strerror(errno));
            exit(1);
        }
    }

    options->minidump_path = argv[optind];
}

// Write all of the given buffer, handling short writes and EINTR. Return true
// iff successful.
static bool writea(int fd, const void * idata, size_t length)
{
    const uint8_t * data = (const uint8_t *)idata;

    size_t done = 0;
    while (done < length)
    {
        ssize_t r;
        do
        {
            r = write(fd, data + done, length - done);
        } while (r == -1 && errno == EINTR);

        if (r < 1)
            return false;
        done += r;
    }

    return true;
}

/* Dynamically determines the byte sex of the system. Returns non-zero
 * for big-endian machines.
 */
static inline int sex()
{
    int probe = 1;
    return !*(char *)&probe;
}

typedef struct elf_timeval
{ /* Time value with microsecond resolution    */
    long tv_sec; /* Seconds                                   */
    long tv_usec; /* Microseconds                              */
} elf_timeval;

typedef struct _elf_siginfo
{ /* Information about signal (unused)         */
    int32_t si_signo; /* Signal number                             */
    int32_t si_code; /* Extra code                                */
    int32_t si_errno; /* Errno                                     */
} _elf_siginfo;

typedef struct prstatus
{ /* Information about thread; includes CPU reg*/
    _elf_siginfo pr_info; /* Info associated with signal               */
    uint16_t pr_cursig; /* Current signal                            */
    unsigned long pr_sigpend; /* Set of pending signals                    */
    unsigned long pr_sighold; /* Set of held signals                       */
    pid_t pr_pid; /* Process ID                                */
    pid_t pr_ppid; /* Parent's process ID                       */
    pid_t pr_pgrp; /* Group ID                                  */
    pid_t pr_sid; /* Session ID                                */
    elf_timeval pr_utime; /* User time                                 */
    elf_timeval pr_stime; /* System time                               */
    elf_timeval pr_cutime; /* Cumulative user time                      */
    elf_timeval pr_cstime; /* Cumulative system time                    */
    user_regs_struct pr_reg; /* CPU registers                             */
    uint32_t pr_fpvalid; /* True if math co-processor being used      */
} prstatus;

typedef struct prpsinfo
{ /* Information about process                 */
    unsigned char pr_state; /* Numeric process state                     */
    char pr_sname; /* Char for pr_state                         */
    unsigned char pr_zomb; /* Zombie                                    */
    signed char pr_nice; /* Nice val                                  */
    unsigned long pr_flag; /* Flags                                     */
#if defined(__x86_64__) || defined(__mips__) || defined(__riscv)
    uint32_t pr_uid; /* User ID                                   */
    uint32_t pr_gid; /* Group ID                                  */
#else
    uint16_t pr_uid; /* User ID                                   */
    uint16_t pr_gid; /* Group ID                                  */
#endif
    pid_t pr_pid; /* Process ID                                */
    pid_t pr_ppid; /* Parent's process ID                       */
    pid_t pr_pgrp; /* Group ID                                  */
    pid_t pr_sid; /* Session ID                                */
    char pr_fname[16]; /* Filename of executable                    */
    char pr_psargs[80]; /* Initial part of arg list                  */
} prpsinfo;

// We parse the minidump file and keep the parsed information in this structure
struct CrashedProcess
{
    CrashedProcess() : exception{-1}, auxv(NULL), auxv_length(0)
    {
        memset(&prps, 0, sizeof(prps));
        prps.pr_sname = 'R';
        memset(&debug, 0, sizeof(debug));
    }

    struct Mapping
    {
        Mapping() : permissions(0xFFFFFFFF), start_address(0), end_address(0), offset(0) { }

        uint32_t permissions;
        uint64_t start_address, end_address, offset;
        // The name we write out to the core.
        string filename;
        string data;
    };
    std::map<uint64_t, Mapping> mappings;

    int fatal_signal;

    struct Thread
    {
        pid_t tid;
#if defined(__mips__) || defined(__riscv)
        mcontext_t mcontext;
#else
        user_regs_struct regs;
#endif
#if defined(__i386__) || defined(__x86_64__)
        user_fpregs_struct fpregs;
#endif
#if defined(__i386__)
        user_fpxregs_struct fpxregs;
#endif
#if defined(__aarch64__)
        user_fpsimd_struct fpregs;
#endif
        uintptr_t stack_addr;
        const uint8_t * stack;
        size_t stack_length;
    };
    std::vector<Thread> threads;
    Thread exception;

    const uint8_t * auxv;
    size_t auxv_length;

    prpsinfo prps;

    // The GUID/filename from MD_MODULE_LIST_STREAM entries.
    // We gather them for merging later on into the list of maps.
    struct Signature
    {
        char guid[40];
        string filename;
    };
    std::map<uintptr_t, Signature> signatures;

    string dynamic_data;
    MDRawDebug debug;
    std::vector<MDRawLinkMap> link_map;
};

#if defined(__i386__)
static uint32_t U32(const uint8_t * data)
{
    uint32_t v;
    memcpy(&v, data, sizeof(v));
    return v;
}

static uint16_t U16(const uint8_t * data)
{
    uint16_t v;
    memcpy(&v, data, sizeof(v));
    return v;
}

static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
    const MDRawContextX86 * rawregs = range.GetData<MDRawContextX86>(0);

    thread->regs.ebx = rawregs->ebx;
    thread->regs.ecx = rawregs->ecx;
    thread->regs.edx = rawregs->edx;
    thread->regs.esi = rawregs->esi;
    thread->regs.edi = rawregs->edi;
    thread->regs.ebp = rawregs->ebp;
    thread->regs.eax = rawregs->eax;
    thread->regs.xds = rawregs->ds;
    thread->regs.xes = rawregs->es;
    thread->regs.xfs = rawregs->fs;
    thread->regs.xgs = rawregs->gs;
    thread->regs.orig_eax = rawregs->eax;
    thread->regs.eip = rawregs->eip;
    thread->regs.xcs = rawregs->cs;
    thread->regs.eflags = rawregs->eflags;
    thread->regs.esp = rawregs->esp;
    thread->regs.xss = rawregs->ss;

    thread->fpregs.cwd = rawregs->float_save.control_word;
    thread->fpregs.swd = rawregs->float_save.status_word;
    thread->fpregs.twd = rawregs->float_save.tag_word;
    thread->fpregs.fip = rawregs->float_save.error_offset;
    thread->fpregs.fcs = rawregs->float_save.error_selector;
    thread->fpregs.foo = rawregs->float_save.data_offset;
    thread->fpregs.fos = rawregs->float_save.data_selector;
    memcpy(thread->fpregs.st_space, rawregs->float_save.register_area, 10 * 8);

    thread->fpxregs.cwd = rawregs->float_save.control_word;
    thread->fpxregs.swd = rawregs->float_save.status_word;
    thread->fpxregs.twd = rawregs->float_save.tag_word;
    thread->fpxregs.fop = U16(rawregs->extended_registers + 6);
    thread->fpxregs.fip = U16(rawregs->extended_registers + 8);
    thread->fpxregs.fcs = U16(rawregs->extended_registers + 12);
    thread->fpxregs.foo = U16(rawregs->extended_registers + 16);
    thread->fpxregs.fos = U16(rawregs->extended_registers + 20);
    thread->fpxregs.mxcsr = U32(rawregs->extended_registers + 24);
    memcpy(thread->fpxregs.st_space, rawregs->extended_registers + 32, 128);
    memcpy(thread->fpxregs.xmm_space, rawregs->extended_registers + 160, 128);
}
#elif defined(__x86_64__)
static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
    const MDRawContextAMD64 * rawregs = range.GetData<MDRawContextAMD64>(0);

    thread->regs.r15 = rawregs->r15;
    thread->regs.r14 = rawregs->r14;
    thread->regs.r13 = rawregs->r13;
    thread->regs.r12 = rawregs->r12;
    thread->regs.rbp = rawregs->rbp;
    thread->regs.rbx = rawregs->rbx;
    thread->regs.r11 = rawregs->r11;
    thread->regs.r10 = rawregs->r10;
    thread->regs.r9 = rawregs->r9;
    thread->regs.r8 = rawregs->r8;
    thread->regs.rax = rawregs->rax;
    thread->regs.rcx = rawregs->rcx;
    thread->regs.rdx = rawregs->rdx;
    thread->regs.rsi = rawregs->rsi;
    thread->regs.rdi = rawregs->rdi;
    thread->regs.orig_rax = rawregs->rax;
    thread->regs.rip = rawregs->rip;
    thread->regs.cs = rawregs->cs;
    thread->regs.eflags = rawregs->eflags;
    thread->regs.rsp = rawregs->rsp;
    thread->regs.ss = rawregs->ss;
    thread->regs.fs_base = 0;
    thread->regs.gs_base = 0;
    thread->regs.ds = rawregs->ds;
    thread->regs.es = rawregs->es;
    thread->regs.fs = rawregs->fs;
    thread->regs.gs = rawregs->gs;

    thread->fpregs.cwd = rawregs->flt_save.control_word;
    thread->fpregs.swd = rawregs->flt_save.status_word;
    thread->fpregs.ftw = rawregs->flt_save.tag_word;
    thread->fpregs.fop = rawregs->flt_save.error_opcode;
    thread->fpregs.rip = rawregs->flt_save.error_offset;
    thread->fpregs.rdp = rawregs->flt_save.data_offset;
    thread->fpregs.mxcsr = rawregs->flt_save.mx_csr;
    thread->fpregs.mxcr_mask = rawregs->flt_save.mx_csr_mask;
    memcpy(thread->fpregs.st_space, rawregs->flt_save.float_registers, 8 * 16);
    memcpy(thread->fpregs.xmm_space, rawregs->flt_save.xmm_registers, 16 * 16);
}
#elif defined(__arm__)
static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
    const MDRawContextARM * rawregs = range.GetData<MDRawContextARM>(0);

    thread->regs.uregs[0] = rawregs->iregs[0];
    thread->regs.uregs[1] = rawregs->iregs[1];
    thread->regs.uregs[2] = rawregs->iregs[2];
    thread->regs.uregs[3] = rawregs->iregs[3];
    thread->regs.uregs[4] = rawregs->iregs[4];
    thread->regs.uregs[5] = rawregs->iregs[5];
    thread->regs.uregs[6] = rawregs->iregs[6];
    thread->regs.uregs[7] = rawregs->iregs[7];
    thread->regs.uregs[8] = rawregs->iregs[8];
    thread->regs.uregs[9] = rawregs->iregs[9];
    thread->regs.uregs[10] = rawregs->iregs[10];
    thread->regs.uregs[11] = rawregs->iregs[11];
    thread->regs.uregs[12] = rawregs->iregs[12];
    thread->regs.uregs[13] = rawregs->iregs[13];
    thread->regs.uregs[14] = rawregs->iregs[14];
    thread->regs.uregs[15] = rawregs->iregs[15];

    thread->regs.uregs[16] = rawregs->cpsr;
    thread->regs.uregs[17] = 0; // what is ORIG_r0 exactly?
}
#elif defined(__aarch64__)
static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
#    define COPY_REGS(rawregs) \
        do \
        { \
            for (int i = 0; i < 31; ++i) \
                thread->regs.regs[i] = rawregs->iregs[i]; \
            thread->regs.sp = rawregs->iregs[MD_CONTEXT_ARM64_REG_SP]; \
            thread->regs.pc = rawregs->iregs[MD_CONTEXT_ARM64_REG_PC]; \
            thread->regs.pstate = rawregs->cpsr; \
\
            memcpy(thread->fpregs.vregs, rawregs->float_save.regs, 8 * 32); \
            thread->fpregs.fpsr = rawregs->float_save.fpsr; \
            thread->fpregs.fpcr = rawregs->float_save.fpcr; \
        } while (false)

    if (range.length() == sizeof(MDRawContextARM64_Old))
    {
        const MDRawContextARM64_Old * rawregs = range.GetData<MDRawContextARM64_Old>(0);
        COPY_REGS(rawregs);
    }
    else
    {
        const MDRawContextARM64 * rawregs = range.GetData<MDRawContextARM64>(0);
        COPY_REGS(rawregs);
    }
#    undef COPY_REGS
}
#elif defined(__mips__)
static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
    const MDRawContextMIPS * rawregs = range.GetData<MDRawContextMIPS>(0);

    for (int i = 0; i < MD_CONTEXT_MIPS_GPR_COUNT; ++i)
        thread->mcontext.gregs[i] = rawregs->iregs[i];

    thread->mcontext.pc = rawregs->epc;

    thread->mcontext.mdlo = rawregs->mdlo;
    thread->mcontext.mdhi = rawregs->mdhi;

    thread->mcontext.hi1 = rawregs->hi[0];
    thread->mcontext.lo1 = rawregs->lo[0];
    thread->mcontext.hi2 = rawregs->hi[1];
    thread->mcontext.lo2 = rawregs->lo[1];
    thread->mcontext.hi3 = rawregs->hi[2];
    thread->mcontext.lo3 = rawregs->lo[2];

    for (int i = 0; i < MD_FLOATINGSAVEAREA_MIPS_FPR_COUNT; ++i)
    {
        thread->mcontext.fpregs.fp_r.fp_fregs[i]._fp_fregs = rawregs->float_save.regs[i];
    }

    thread->mcontext.fpc_csr = rawregs->float_save.fpcsr;
#    if _MIPS_SIM == _ABIO32
    thread->mcontext.fpc_eir = rawregs->float_save.fir;
#    endif
}
#elif defined(__riscv)
static void ParseThreadRegisters(CrashedProcess::Thread * thread, const MinidumpMemoryRange & range)
{
#    if __riscv_xlen == 32
    const MDRawContextRISCV * rawregs = range.GetData<MDRawContextRISCV>(0);
#    elif __riscv_xlen == 64
    const MDRawContextRISCV64 * rawregs = range.GetData<MDRawContextRISCV64>(0);
#    else
#        error "Unexpected __riscv_xlen"
#    endif

    thread->mcontext.__gregs[0] = rawregs->pc;
    thread->mcontext.__gregs[1] = rawregs->ra;
    thread->mcontext.__gregs[2] = rawregs->sp;
    thread->mcontext.__gregs[3] = rawregs->gp;
    thread->mcontext.__gregs[4] = rawregs->tp;
    thread->mcontext.__gregs[5] = rawregs->t0;
    thread->mcontext.__gregs[6] = rawregs->t1;
    thread->mcontext.__gregs[7] = rawregs->t2;
    thread->mcontext.__gregs[8] = rawregs->s0;
    thread->mcontext.__gregs[9] = rawregs->s1;
    thread->mcontext.__gregs[10] = rawregs->a0;
    thread->mcontext.__gregs[11] = rawregs->a1;
    thread->mcontext.__gregs[12] = rawregs->a2;
    thread->mcontext.__gregs[13] = rawregs->a3;
    thread->mcontext.__gregs[14] = rawregs->a4;
    thread->mcontext.__gregs[15] = rawregs->a5;
    thread->mcontext.__gregs[16] = rawregs->a6;
    thread->mcontext.__gregs[17] = rawregs->a7;
    thread->mcontext.__gregs[18] = rawregs->s2;
    thread->mcontext.__gregs[19] = rawregs->s3;
    thread->mcontext.__gregs[20] = rawregs->s4;
    thread->mcontext.__gregs[21] = rawregs->s5;
    thread->mcontext.__gregs[22] = rawregs->s6;
    thread->mcontext.__gregs[23] = rawregs->s7;
    thread->mcontext.__gregs[24] = rawregs->s8;
    thread->mcontext.__gregs[25] = rawregs->s9;
    thread->mcontext.__gregs[26] = rawregs->s10;
    thread->mcontext.__gregs[27] = rawregs->s11;
    thread->mcontext.__gregs[28] = rawregs->t3;
    thread->mcontext.__gregs[29] = rawregs->t4;
    thread->mcontext.__gregs[30] = rawregs->t5;
    thread->mcontext.__gregs[31] = rawregs->t6;

    // Breakpad only supports RISCV32 with 32 bit floating point.
    // Breakpad only supports RISCV64 with 64 bit floating point.
#    if __riscv_xlen == 32
    for (int i = 0; i < MD_CONTEXT_RISCV_FPR_COUNT; ++i)
    {
        thread->mcontext.__fpregs.__f.__f[i] = rawregs->fpregs[i];
    }
    thread->mcontext.__fpregs.__f.__fcsr = rawregs->fcsr;
#    elif __riscv_xlen == 64
    for (int i = 0; i < MD_CONTEXT_RISCV_FPR_COUNT; ++i)
    {
        thread->mcontext.__fpregs.__d.__f[i] = rawregs->fpregs[i];
    }
    thread->mcontext.__fpregs.__d.__fcsr = rawregs->fcsr;
#    else
#        error "Unexpected __riscv_xlen"
#    endif
}
#else
#    error "This code has not been ported to your platform yet"
#endif

static void ParseThreadList(
    const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range, const MinidumpMemoryRange & full_file)
{
    const uint32_t num_threads = *range.GetData<uint32_t>(0);
    if (options.verbose)
    {
        fprintf(
            stderr,
            "MD_THREAD_LIST_STREAM:\n"
            "Found %d threads\n"
            "\n\n",
            num_threads);
    }
    for (unsigned i = 0; i < num_threads; ++i)
    {
        CrashedProcess::Thread thread;
        memset(&thread, 0, sizeof(thread));
        const MDRawThread * rawthread = range.GetArrayElement<MDRawThread>(sizeof(uint32_t), i);
        thread.tid = rawthread->thread_id;
        thread.stack_addr = rawthread->stack.start_of_memory_range;
        MinidumpMemoryRange stack_range = full_file.Subrange(rawthread->stack.memory);
        thread.stack = stack_range.data();
        thread.stack_length = rawthread->stack.memory.data_size;

        ParseThreadRegisters(&thread, full_file.Subrange(rawthread->thread_context));

        crashinfo->threads.push_back(thread);
    }
}

static void ParseSystemInfo(
    const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range, const MinidumpMemoryRange & full_file)
{
    const MDRawSystemInfo * sysinfo = range.GetData<MDRawSystemInfo>(0);
    if (!sysinfo)
    {
        fprintf(stderr, "Failed to access MD_SYSTEM_INFO_STREAM\n");
        exit(1);
    }
#if defined(__i386__)
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_X86)
    {
        fprintf(
            stderr,
            "This version of minidump-2-core only supports x86 (32bit)%s.\n",
            sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_AMD64 ? ",\nbut the minidump file is from a 64bit machine" : "");
        exit(1);
    }
#elif defined(__x86_64__)
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_AMD64)
    {
        fprintf(
            stderr,
            "This version of minidump-2-core only supports x86 (64bit)%s.\n",
            sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_X86 ? ",\nbut the minidump file is from a 32bit machine" : "");
        exit(1);
    }
#elif defined(__arm__)
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_ARM)
    {
        fprintf(stderr, "This version of minidump-2-core only supports ARM (32bit).\n");
        exit(1);
    }
#elif defined(__aarch64__)
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_ARM64_OLD && sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_ARM64)
    {
        fprintf(stderr, "This version of minidump-2-core only supports ARM (64bit).\n");
        exit(1);
    }
#elif defined(__mips__)
#    if _MIPS_SIM == _ABIO32
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_MIPS)
    {
        fprintf(stderr, "This version of minidump-2-core only supports mips o32 (32bit).\n");
        exit(1);
    }
#    elif _MIPS_SIM == _ABI64
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_MIPS64)
    {
        fprintf(stderr, "This version of minidump-2-core only supports mips n64 (64bit).\n");
        exit(1);
    }
#    else
#        error "This mips ABI is currently not supported (n32)"
#    endif
#elif defined(__riscv)
#    if __riscv_xlen == 32
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_RISCV)
    {
        fprintf(stderr, "This version of minidump-2-core only supports RISCV.\n");
        exit(1);
    }
#    elif __riscv_xlen == 64
    if (sysinfo->processor_architecture != MD_CPU_ARCHITECTURE_RISCV64)
    {
        fprintf(stderr, "This version of minidump-2-core only supports RISCV64.\n");
        exit(1);
    }
#    else
#        error "Unexpected __riscv_xlen"
#    endif
#else
#    error "This code has not been ported to your platform yet"
#endif
    if (sysinfo->platform_id != MD_OS_LINUX && sysinfo->platform_id != MD_OS_NACL)
    {
        fprintf(stderr, "This minidump was not generated by Linux or NaCl.\n");
        exit(1);
    }

    if (options.verbose)
    {
        fprintf(
            stderr,
            "MD_SYSTEM_INFO_STREAM:\n"
            "Architecture: %s\n"
            "Number of processors: %d\n"
            "Processor level: %d\n"
            "Processor model: %d\n"
            "Processor stepping: %d\n",
            sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_X86           ? "i386"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_AMD64   ? "x86-64"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_ARM     ? "ARM"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_MIPS    ? "MIPS"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_MIPS64  ? "MIPS64"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_RISCV   ? "RISCV"
                : sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_RISCV64 ? "RISCV64"
                                                                                 : "???",
            sysinfo->number_of_processors,
            sysinfo->processor_level,
            sysinfo->processor_revision >> 8,
            sysinfo->processor_revision & 0xFF);
        if (sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_X86 || sysinfo->processor_architecture == MD_CPU_ARCHITECTURE_AMD64)
        {
            fputs("Vendor id: ", stderr);
            const char * nul = (const char *)memchr(sysinfo->cpu.x86_cpu_info.vendor_id, 0, sizeof(sysinfo->cpu.x86_cpu_info.vendor_id));
            fwrite(
                sysinfo->cpu.x86_cpu_info.vendor_id,
                nul ? nul - (const char *)&sysinfo->cpu.x86_cpu_info.vendor_id[0] : sizeof(sysinfo->cpu.x86_cpu_info.vendor_id),
                1,
                stderr);
            fputs("\n", stderr);
        }
        fprintf(stderr, "OS: %s\n", full_file.GetAsciiMDString(sysinfo->csd_version_rva).c_str());
        fputs("\n\n", stderr);
    }
}

static void ParseCPUInfo(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    if (options.verbose)
    {
        fputs("MD_LINUX_CPU_INFO:\n", stderr);
        fwrite(range.data(), range.length(), 1, stderr);
        fputs("\n\n\n", stderr);
    }
}

static void ParseProcessStatus(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    if (options.verbose)
    {
        fputs("MD_LINUX_PROC_STATUS:\n", stderr);
        fwrite(range.data(), range.length(), 1, stderr);
        fputs("\n\n", stderr);
    }
}

static void ParseLSBRelease(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    if (options.verbose)
    {
        fputs("MD_LINUX_LSB_RELEASE:\n", stderr);
        fwrite(range.data(), range.length(), 1, stderr);
        fputs("\n\n", stderr);
    }
}

static void ParseMaps(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    if (options.verbose)
    {
        fputs("MD_LINUX_MAPS:\n", stderr);
        fwrite(range.data(), range.length(), 1, stderr);
    }
    for (const uint8_t * ptr = range.data(); ptr < range.data() + range.length();)
    {
        const uint8_t * eol = (uint8_t *)memchr(ptr, '\n', range.data() + range.length() - ptr);
        string line((const char *)ptr, eol ? eol - ptr : range.data() + range.length() - ptr);
        ptr = eol ? eol + 1 : range.data() + range.length();
        unsigned long long start, stop, offset;
        char * permissions = NULL;
        char * filename = NULL;
        sscanf(line.c_str(), "%llx-%llx %m[-rwxp] %llx %*[:0-9a-f] %*d %ms", &start, &stop, &permissions, &offset, &filename);
        if (filename && *filename == '/')
        {
            CrashedProcess::Mapping mapping;
            mapping.permissions = 0;
            if (strchr(permissions, 'r'))
            {
                mapping.permissions |= PF_R;
            }
            if (strchr(permissions, 'w'))
            {
                mapping.permissions |= PF_W;
            }
            if (strchr(permissions, 'x'))
            {
                mapping.permissions |= PF_X;
            }
            mapping.start_address = start;
            mapping.end_address = stop;
            mapping.offset = offset;
            if (filename)
            {
                mapping.filename = filename;
            }
            crashinfo->mappings[mapping.start_address] = mapping;
        }
        free(permissions);
        free(filename);
    }
    if (options.verbose)
    {
        fputs("\n\n\n", stderr);
    }
}

static void ParseEnvironment(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    if (options.verbose)
    {
        fputs("MD_LINUX_ENVIRON:\n", stderr);
        char * env = new char[range.length()];
        memcpy(env, range.data(), range.length());
        int nul_count = 0;
        for (char * ptr = env;;)
        {
            ptr = (char *)memchr(ptr, '\000', range.length() - (ptr - env));
            if (!ptr)
            {
                break;
            }
            if (ptr > env && ptr[-1] == '\n')
            {
                if (++nul_count > 5)
                {
                    // Some versions of Chrome try to rewrite the process' command line
                    // in a way that causes the environment to be corrupted. Afterwards,
                    // part of the environment will contain the trailing bit of the
                    // command line. The rest of the environment will be filled with
                    // NUL bytes.
                    // We detect this corruption by counting the number of consecutive
                    // NUL bytes. Normally, we would not expect any consecutive NUL
                    // bytes. But we are conservative and only suppress printing of
                    // the environment if we see at least five consecutive NULs.
                    fputs("Environment has been corrupted; no data available", stderr);
                    goto env_corrupted;
                }
            }
            else
            {
                nul_count = 0;
            }
            *ptr = '\n';
        }
        fwrite(env, range.length(), 1, stderr);
    env_corrupted:
        delete[] env;
        fputs("\n\n\n", stderr);
    }
}

static void ParseAuxVector(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    // Some versions of Chrome erroneously used the MD_LINUX_AUXV stream value
    // when dumping /proc/$x/maps
    if (range.length() > 17)
    {
        // The AUXV vector contains binary data, whereas the maps always begin
        // with an 8+ digit hex address followed by a hyphen and another 8+ digit
        // address.
        char addresses[18];
        memcpy(addresses, range.data(), 17);
        addresses[17] = '\000';
        if (strspn(addresses, "0123456789abcdef-") == 17)
        {
            ParseMaps(options, crashinfo, range);
            return;
        }
    }

    crashinfo->auxv = range.data();
    crashinfo->auxv_length = range.length();
}

static void ParseCmdLine(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range)
{
    // The command line is supposed to use NUL bytes to separate arguments.
    // As Chrome rewrites its own command line and (incorrectly) substitutes
    // spaces, this is often not the case in our minidump files.
    const char * cmdline = (const char *)range.data();
    if (options.verbose)
    {
        fputs("MD_LINUX_CMD_LINE:\n", stderr);
        unsigned i = 0;
        for (; i < range.length() && cmdline[i] && cmdline[i] != ' '; ++i)
        {
        }
        fputs("argv[0] = \"", stderr);
        fwrite(cmdline, i, 1, stderr);
        fputs("\"\n", stderr);
        for (unsigned j = ++i, argc = 1; j < range.length(); ++j)
        {
            if (!cmdline[j] || cmdline[j] == ' ')
            {
                fprintf(stderr, "argv[%d] = \"", argc++);
                fwrite(cmdline + i, j - i, 1, stderr);
                fputs("\"\n", stderr);
                i = j + 1;
            }
        }
        fputs("\n\n", stderr);
    }

    const char * binary_name = cmdline;
    for (size_t i = 0; i < range.length(); ++i)
    {
        if (cmdline[i] == '/')
        {
            binary_name = cmdline + i + 1;
        }
        else if (cmdline[i] == 0 || cmdline[i] == ' ')
        {
            static const size_t fname_len = sizeof(crashinfo->prps.pr_fname) - 1;
            static const size_t args_len = sizeof(crashinfo->prps.pr_psargs) - 1;
            memset(crashinfo->prps.pr_fname, 0, fname_len + 1);
            memset(crashinfo->prps.pr_psargs, 0, args_len + 1);
            unsigned len = cmdline + i - binary_name;
            memcpy(crashinfo->prps.pr_fname, binary_name, len > fname_len ? fname_len : len);

            len = range.length() > args_len ? args_len : range.length();
            memcpy(crashinfo->prps.pr_psargs, cmdline, len);
            for (unsigned j = 0; j < len; ++j)
            {
                if (crashinfo->prps.pr_psargs[j] == 0)
                    crashinfo->prps.pr_psargs[j] = ' ';
            }
            break;
        }
    }
}

static void ParseDSODebugInfo(
    const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range, const MinidumpMemoryRange & full_file)
{
    const MDRawDebug * debug = range.GetData<MDRawDebug>(0);
    if (!debug)
    {
        return;
    }
    if (options.verbose)
    {
        fprintf(
            stderr,
            "MD_LINUX_DSO_DEBUG:\n"
            "Version: %d\n"
            "Number of DSOs: %d\n"
            "Brk handler: 0x%" PRIx64 "\n"
            "Dynamic loader at: 0x%" PRIx64 "\n"
            "_DYNAMIC: 0x%" PRIx64 "\n",
            debug->version,
            debug->dso_count,
            static_cast<uint64_t>(debug->brk),
            static_cast<uint64_t>(debug->ldbase),
            static_cast<uint64_t>(debug->dynamic));
    }
    crashinfo->debug = *debug;
    if (range.length() > sizeof(MDRawDebug))
    {
        char * dynamic_data = (char *)range.data() + sizeof(MDRawDebug);
        crashinfo->dynamic_data.assign(dynamic_data, range.length() - sizeof(MDRawDebug));
    }
    if (debug->map != kInvalidMDRVA)
    {
        for (unsigned int i = 0; i < debug->dso_count; ++i)
        {
            const MDRawLinkMap * link_map = full_file.GetArrayElement<MDRawLinkMap>(debug->map, i);
            if (link_map)
            {
                if (options.verbose)
                {
                    fprintf(
                        stderr,
                        "#%03d: %" PRIx64 ", %" PRIx64 ", \"%s\"\n",
                        i,
                        static_cast<uint64_t>(link_map->addr),
                        static_cast<uint64_t>(link_map->ld),
                        full_file.GetAsciiMDString(link_map->name).c_str());
                }
                crashinfo->link_map.push_back(*link_map);
            }
        }
    }
    if (options.verbose)
    {
        fputs("\n\n", stderr);
    }
}

static void ParseExceptionStream(
    const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range, const MinidumpMemoryRange & full_file)
{
    const MDRawExceptionStream * exp = range.GetData<MDRawExceptionStream>(0);
    if (!exp)
    {
        return;
    }
    if (options.verbose)
    {
        fprintf(
            stderr,
            "MD_EXCEPTION_STREAM:\n"
            "Found exception thread %" PRIu32 " \n"
            "\n\n",
            exp->thread_id);
    }
    crashinfo->fatal_signal = (int)exp->exception_record.exception_code;
    crashinfo->exception = {};
    crashinfo->exception.tid = exp->thread_id;
    // crashinfo->threads[].tid == crashinfo->exception.tid provides the stack.
    ParseThreadRegisters(&crashinfo->exception, full_file.Subrange(exp->thread_context));
}

static bool WriteThread(const Options & options, const CrashedProcess::Thread & thread, int fatal_signal)
{
    struct prstatus pr;
    memset(&pr, 0, sizeof(pr));

    pr.pr_info.si_signo = fatal_signal;
    pr.pr_cursig = fatal_signal;
    pr.pr_pid = thread.tid;
#if defined(__mips__)
    memcpy(&pr.pr_reg, &thread.mcontext.gregs, sizeof(user_regs_struct));
#elif defined(__riscv)
    memcpy(&pr.pr_reg, &thread.mcontext.__gregs, sizeof(user_regs_struct));
#else
    memcpy(&pr.pr_reg, &thread.regs, sizeof(user_regs_struct));
#endif

    Nhdr nhdr;
    memset(&nhdr, 0, sizeof(nhdr));
    nhdr.n_namesz = 5;
    nhdr.n_descsz = sizeof(struct prstatus);
    nhdr.n_type = NT_PRSTATUS;
    if (!writea(options.out_fd, &nhdr, sizeof(nhdr)) || !writea(options.out_fd, "CORE\0\0\0\0", 8)
        || !writea(options.out_fd, &pr, sizeof(struct prstatus)))
    {
        return false;
    }

#if defined(__i386__) || defined(__x86_64__)
    nhdr.n_descsz = sizeof(user_fpregs_struct);
    nhdr.n_type = NT_FPREGSET;
    if (!writea(options.out_fd, &nhdr, sizeof(nhdr)) || !writea(options.out_fd, "CORE\0\0\0\0", 8)
        || !writea(options.out_fd, &thread.fpregs, sizeof(user_fpregs_struct)))
    {
        return false;
    }
#endif

#if defined(__i386__)
    nhdr.n_descsz = sizeof(user_fpxregs_struct);
    nhdr.n_type = NT_PRXFPREG;
    if (!writea(options.out_fd, &nhdr, sizeof(nhdr)) || !writea(options.out_fd, "LINUX\0\0\0", 8)
        || !writea(options.out_fd, &thread.fpxregs, sizeof(user_fpxregs_struct)))
    {
        return false;
    }
#endif

    return true;
}

static void ParseModuleStream(
    const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & range, const MinidumpMemoryRange & full_file)
{
    if (options.verbose)
    {
        fputs("MD_MODULE_LIST_STREAM:\n", stderr);
    }
    const uint32_t num_mappings = *range.GetData<uint32_t>(0);
    for (unsigned i = 0; i < num_mappings; ++i)
    {
        CrashedProcess::Mapping mapping;
        const MDRawModule * rawmodule = reinterpret_cast<const MDRawModule *>(range.GetArrayElement(sizeof(uint32_t), MD_MODULE_SIZE, i));
        mapping.start_address = rawmodule->base_of_image;
        mapping.end_address = rawmodule->size_of_image + rawmodule->base_of_image;

        if (crashinfo->mappings.find(mapping.start_address) == crashinfo->mappings.end())
        {
            // We prefer data from MD_LINUX_MAPS over MD_MODULE_LIST_STREAM, as
            // the former is a strict superset of the latter.
            crashinfo->mappings[mapping.start_address] = mapping;
        }

        const MDCVInfoPDB70 * record
            = reinterpret_cast<const MDCVInfoPDB70 *>(full_file.GetData(rawmodule->cv_record.rva, MDCVInfoPDB70_minsize));
        char guid[40];
        sprintf(
            guid,
            "%08X-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X",
            record->signature.data1,
            record->signature.data2,
            record->signature.data3,
            record->signature.data4[0],
            record->signature.data4[1],
            record->signature.data4[2],
            record->signature.data4[3],
            record->signature.data4[4],
            record->signature.data4[5],
            record->signature.data4[6],
            record->signature.data4[7]);

        string filename = full_file.GetAsciiMDString(rawmodule->module_name_rva);

        CrashedProcess::Signature signature;
        strcpy(signature.guid, guid);
        signature.filename = filename;
        crashinfo->signatures[rawmodule->base_of_image] = signature;

        if (options.verbose)
        {
            fprintf(
                stderr,
                "0x%" PRIx64 "-0x%" PRIx64 ", ChkSum: 0x%08X, GUID: %s, "
                " \"%s\"\n",
                rawmodule->base_of_image,
                rawmodule->base_of_image + rawmodule->size_of_image,
                rawmodule->checksum,
                guid,
                filename.c_str());
        }
    }
    if (options.verbose)
    {
        fputs("\n\n", stderr);
    }
}

static void AddDataToMapping(CrashedProcess * crashinfo, const string & data, uintptr_t addr)
{
    for (std::map<uint64_t, CrashedProcess::Mapping>::iterator iter = crashinfo->mappings.begin(); iter != crashinfo->mappings.end();
         ++iter)
    {
        if (addr >= iter->second.start_address && addr < iter->second.end_address)
        {
            CrashedProcess::Mapping mapping = iter->second;
            if ((addr & ~4095) != iter->second.start_address)
            {
                // If there are memory pages in the mapping prior to where the
                // data starts, truncate the existing mapping so that it ends with
                // the page immediately preceding the data region.
                iter->second.end_address = addr & ~4095;
                if (!mapping.filename.empty())
                {
                    // "mapping" is a copy of "iter->second". We are splitting the
                    // existing mapping into two separate ones when we write the data
                    // to the core file. The first one does not have any associated
                    // data in the core file, the second one is backed by data that is
                    // included with the core file.
                    // If this mapping wasn't supposed to be anonymous, then we also
                    // have to update the file offset upon splitting the mapping.
                    mapping.offset += iter->second.end_address - iter->second.start_address;
                }
            }
            // Create a new mapping that contains the data contents. We often
            // limit the amount of data that is actually written to the core
            // file. But it is OK if the mapping itself extends past the end of
            // the data.
            mapping.start_address = addr & ~4095;
            mapping.data.assign(addr & 4095, 0).append(data);
            mapping.data.append(-mapping.data.size() & 4095, 0);
            crashinfo->mappings[mapping.start_address] = mapping;
            return;
        }
    }
    // Didn't find a suitable existing mapping for the data. Create a new one.
    CrashedProcess::Mapping mapping;
    mapping.permissions = PF_R | PF_W;
    mapping.start_address = addr & ~4095;
    mapping.end_address = (addr + data.size() + 4095) & ~4095;
    mapping.data.assign(addr & 4095, 0).append(data);
    mapping.data.append(-mapping.data.size() & 4095, 0);
    crashinfo->mappings[mapping.start_address] = mapping;
}

static void AugmentMappings(const Options & options, CrashedProcess * crashinfo, const MinidumpMemoryRange & full_file)
{
    // For each thread, find the memory mapping that matches the thread's stack.
    // Then adjust the mapping to include the stack dump.
    for (unsigned i = 0; i < crashinfo->threads.size(); ++i)
    {
        const CrashedProcess::Thread & thread = crashinfo->threads[i];
        AddDataToMapping(crashinfo, string((char *)thread.stack, thread.stack_length), thread.stack_addr);
    }

    // Create a new link map with information about DSOs. We move this map to
    // the beginning of the address space, as this area should always be
    // available.
    static const uintptr_t start_addr = 4096;
    string data;
    struct r_debug debug = {0};
    debug.r_version = crashinfo->debug.version;
    debug.r_brk = (ElfW(Addr))crashinfo->debug.brk;
    debug.r_state = r_debug::RT_CONSISTENT;
    debug.r_ldbase = (ElfW(Addr))crashinfo->debug.ldbase;
    debug.r_map = crashinfo->debug.dso_count > 0 ? (struct link_map *)(start_addr + sizeof(debug)) : 0;
    data.append((char *)&debug, sizeof(debug));

    struct link_map * prev = 0;
    for (std::vector<MDRawLinkMap>::iterator iter = crashinfo->link_map.begin(); iter != crashinfo->link_map.end(); ++iter)
    {
        struct link_map link_map = {0};
        link_map.l_addr = (ElfW(Addr))iter->addr;
        link_map.l_name = (char *)(start_addr + data.size() + sizeof(link_map));
        link_map.l_ld = (ElfW(Dyn) *)iter->ld;
        link_map.l_prev = prev;
        prev = (struct link_map *)(start_addr + data.size());
        string filename = full_file.GetAsciiMDString(iter->name);

        // Look up signature for this filename. If available, change filename
        // to point to GUID, instead.
        std::map<uintptr_t, CrashedProcess::Signature>::const_iterator sig = crashinfo->signatures.find((uintptr_t)iter->addr);
        if (sig != crashinfo->signatures.end())
        {
            // At this point, we have:
            // old_filename: The path as found via SONAME (e.g. /lib/libpthread.so.0).
            // sig_filename: The path on disk (e.g. /lib/libpthread-2.19.so).
            const char * guid = sig->second.guid;
            string sig_filename = sig->second.filename;
            string old_filename = filename.empty() ? sig_filename : filename;
            string new_filename;

            // First set up the leading path.  We assume dirname always ends with a
            // trailing slash (as needed), so we won't be appending one manually.
            if (options.so_basedir.empty())
            {
                string dirname;
                if (options.use_filename)
                {
                    dirname = sig_filename;
                }
                else
                {
                    dirname = old_filename;
                }
                size_t slash = dirname.find_last_of('/');
                if (slash != string::npos)
                {
                    new_filename = dirname.substr(0, slash + 1);
                }
            }
            else
            {
                new_filename = options.so_basedir;
            }

            // Insert the module ID if requested.
            if (options.inc_guid && strcmp(guid, "00000000-0000-0000-0000-000000000000") != 0)
            {
                new_filename += guid;
                new_filename += "-";
            }

            // Decide whether we use the filename or the SONAME (where the SONAME tends
            // to be a symlink to the actual file).
            new_filename += google_breakpad::BaseName(options.use_filename ? sig_filename : old_filename);

            if (filename != new_filename)
            {
                if (options.verbose)
                {
                    fprintf(
                        stderr,
                        "0x%" PRIx64 ": rewriting mapping \"%s\" to \"%s\"\n",
                        static_cast<uint64_t>(link_map.l_addr),
                        filename.c_str(),
                        new_filename.c_str());
                }
                filename = new_filename;
            }
        }

        if (std::distance(iter, crashinfo->link_map.end()) == 1)
        {
            link_map.l_next = 0;
        }
        else
        {
            link_map.l_next = (struct link_map *)(start_addr + data.size() + sizeof(link_map) + ((filename.size() + 8) & ~7));
        }
        data.append((char *)&link_map, sizeof(link_map));
        data.append(filename);
        data.append(8 - (filename.size() & 7), 0);
    }
    AddDataToMapping(crashinfo, data, start_addr);

    // Map the page containing the _DYNAMIC array
    if (!crashinfo->dynamic_data.empty())
    {
        // Make _DYNAMIC DT_DEBUG entry point to our link map
        for (int i = 0;; ++i)
        {
            ElfW(Dyn) dyn;
            if ((i + 1) * sizeof(dyn) > crashinfo->dynamic_data.length())
            {
            no_dt_debug:
                if (options.verbose)
                {
                    fprintf(stderr, "No DT_DEBUG entry found\n");
                }
                return;
            }
            memcpy(&dyn, crashinfo->dynamic_data.c_str() + i * sizeof(dyn), sizeof(dyn));
            if (dyn.d_tag == DT_DEBUG)
            {
                crashinfo->dynamic_data.replace(
                    i * sizeof(dyn) + offsetof(ElfW(Dyn), d_un.d_ptr), sizeof(start_addr), (char *)&start_addr, sizeof(start_addr));
                break;
            }
            else if (dyn.d_tag == DT_NULL)
            {
                goto no_dt_debug;
            }
        }
        AddDataToMapping(crashinfo, crashinfo->dynamic_data, (uintptr_t)crashinfo->debug.dynamic);
    }
}


namespace Minidump2Core
{
/// Mimic minidump-2-core.cc to generate coredump from minidump, but run in-process
int generate(const char * minidump_path, const char * coredump_path)
{
    MemoryMappedFile mapped_file(minidump_path, 0);
    if (!mapped_file.data())
    {
        fprintf(stderr, "Failed to mmap dump file: %s: %s\n", minidump_path, strerror(errno));
        return 1;
    }

    Options options;
    options.out_fd = open(coredump_path, O_WRONLY | O_CREAT | O_TRUNC, 0664);
    options.verbose = false;
    if (options.out_fd == -1)
    {
        fprintf(stderr, "Could not open output %s: %s\n", coredump_path, strerror(errno));
        return 1;
    }

    MinidumpMemoryRange dump(mapped_file.data(), mapped_file.size());

    const MDRawHeader * header = dump.GetData<MDRawHeader>(0);

    CrashedProcess crashinfo;

    // Always check the system info first, as that allows us to tell whether
    // this is a minidump file that is compatible with our converter.
    bool ok = false;
    for (unsigned i = 0; i < header->stream_count; ++i)
    {
        const MDRawDirectory * dirent = dump.GetArrayElement<MDRawDirectory>(header->stream_directory_rva, i);
        switch (dirent->stream_type)
        {
            case MD_SYSTEM_INFO_STREAM:
                ParseSystemInfo(options, &crashinfo, dump.Subrange(dirent->location), dump);
                ok = true;
                break;
            default:
                break;
        }
    }
    if (!ok)
    {
        fprintf(stderr, "Cannot determine input file format.\n");
        return 1;
    }

    for (unsigned i = 0; i < header->stream_count; ++i)
    {
        const MDRawDirectory * dirent = dump.GetArrayElement<MDRawDirectory>(header->stream_directory_rva, i);
        switch (dirent->stream_type)
        {
            case MD_THREAD_LIST_STREAM:
                ParseThreadList(options, &crashinfo, dump.Subrange(dirent->location), dump);
                break;
            case MD_LINUX_CPU_INFO:
                ParseCPUInfo(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_PROC_STATUS:
                ParseProcessStatus(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_LSB_RELEASE:
                ParseLSBRelease(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_ENVIRON:
                ParseEnvironment(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_MAPS:
                ParseMaps(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_AUXV:
                ParseAuxVector(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_CMD_LINE:
                ParseCmdLine(options, &crashinfo, dump.Subrange(dirent->location));
                break;
            case MD_LINUX_DSO_DEBUG:
                ParseDSODebugInfo(options, &crashinfo, dump.Subrange(dirent->location), dump);
                break;
            case MD_EXCEPTION_STREAM:
                ParseExceptionStream(options, &crashinfo, dump.Subrange(dirent->location), dump);
                break;
            case MD_MODULE_LIST_STREAM:
                ParseModuleStream(options, &crashinfo, dump.Subrange(dirent->location), dump);
                break;
            default:
                if (options.verbose)
                    fprintf(stderr, "Skipping %x\n", dirent->stream_type);
        }
    }

    AugmentMappings(options, &crashinfo, dump);

    // Write the ELF header. The file will look like:
    //   ELF header
    //   Phdr for the PT_NOTE
    //   Phdr for each of the thread stacks
    //   PT_NOTE
    //   each of the thread stacks
    Ehdr ehdr;
    memset(&ehdr, 0, sizeof(Ehdr));
    ehdr.e_ident[0] = ELFMAG0;
    ehdr.e_ident[1] = ELFMAG1;
    ehdr.e_ident[2] = ELFMAG2;
    ehdr.e_ident[3] = ELFMAG3;
    ehdr.e_ident[4] = ELF_CLASS;
    ehdr.e_ident[5] = sex() ? ELFDATA2MSB : ELFDATA2LSB;
    ehdr.e_ident[6] = EV_CURRENT;
    ehdr.e_type = ET_CORE;
    ehdr.e_machine = ELF_ARCH;
    ehdr.e_version = EV_CURRENT;
    ehdr.e_phoff = sizeof(Ehdr);
    ehdr.e_ehsize = sizeof(Ehdr);
    ehdr.e_phentsize = sizeof(Phdr);
    ehdr.e_phnum = 1 + // PT_NOTE
        crashinfo.mappings.size(); // memory mappings
    ehdr.e_shentsize = sizeof(Shdr);
    if (!writea(options.out_fd, &ehdr, sizeof(Ehdr)))
        return 1;

    size_t offset = sizeof(Ehdr) + ehdr.e_phnum * sizeof(Phdr);
    size_t filesz = sizeof(Nhdr) + 8 + sizeof(prpsinfo) +
        // sizeof(Nhdr) + 8 + sizeof(user) +
        sizeof(Nhdr) + 8 + crashinfo.auxv_length
        + crashinfo.threads.size()
            * ((sizeof(Nhdr) + 8 + sizeof(prstatus))
#if defined(__i386__) || defined(__x86_64__)
               + sizeof(Nhdr) + 8 + sizeof(user_fpregs_struct)
#endif
#if defined(__i386__)
               + sizeof(Nhdr) + 8 + sizeof(user_fpxregs_struct)
#endif
            );

    Phdr phdr;
    memset(&phdr, 0, sizeof(Phdr));
    phdr.p_type = PT_NOTE;
    phdr.p_offset = offset;
    phdr.p_filesz = filesz;
    if (!writea(options.out_fd, &phdr, sizeof(phdr)))
        return 1;

    phdr.p_type = PT_LOAD;
    phdr.p_align = 4096;
    size_t note_align = phdr.p_align - ((offset + filesz) % phdr.p_align);
    if (note_align == phdr.p_align)
        note_align = 0;
    offset += note_align;

    for (std::map<uint64_t, CrashedProcess::Mapping>::const_iterator iter = crashinfo.mappings.begin(); iter != crashinfo.mappings.end();
         ++iter)
    {
        const CrashedProcess::Mapping & mapping = iter->second;
        if (mapping.permissions == 0xFFFFFFFF)
        {
            // This is a map that we found in MD_MODULE_LIST_STREAM (as opposed to
            // MD_LINUX_MAPS). It lacks some of the information that we would like
            // to include.
            phdr.p_flags = PF_R;
        }
        else
        {
            phdr.p_flags = mapping.permissions;
        }
        phdr.p_vaddr = mapping.start_address;
        phdr.p_memsz = mapping.end_address - mapping.start_address;
        if (mapping.data.size())
        {
            offset += filesz;
            filesz = mapping.data.size();
            phdr.p_filesz = mapping.data.size();
            phdr.p_offset = offset;
        }
        else
        {
            phdr.p_filesz = 0;
            phdr.p_offset = 0;
        }
        if (!writea(options.out_fd, &phdr, sizeof(phdr)))
            return 1;
    }

    Nhdr nhdr;
    memset(&nhdr, 0, sizeof(nhdr));
    nhdr.n_namesz = 5;
    nhdr.n_descsz = sizeof(prpsinfo);
    nhdr.n_type = NT_PRPSINFO;
    if (!writea(options.out_fd, &nhdr, sizeof(nhdr)) || !writea(options.out_fd, "CORE\0\0\0\0", 8)
        || !writea(options.out_fd, &crashinfo.prps, sizeof(prpsinfo)))
    {
        return 1;
    }

    nhdr.n_descsz = crashinfo.auxv_length;
    nhdr.n_type = NT_AUXV;
    if (!writea(options.out_fd, &nhdr, sizeof(nhdr)) || !writea(options.out_fd, "CORE\0\0\0\0", 8)
        || !writea(options.out_fd, crashinfo.auxv, crashinfo.auxv_length))
    {
        return 1;
    }

    for (const auto & current_thread : crashinfo.threads)
    {
        if (current_thread.tid == crashinfo.exception.tid)
        {
            // Use the exception record's context for the crashed thread instead of
            // the thread's own context. For the crashed thread the thread's own
            // context is the state inside the exception handler. Using it would not
            // result in the expected stack trace from the time of the crash.
            // The stack memory has already been provided by current_thread.
            WriteThread(options, crashinfo.exception, crashinfo.fatal_signal);
            break;
        }
    }

    for (const auto & current_thread : crashinfo.threads)
    {
        if (current_thread.tid != crashinfo.exception.tid)
            WriteThread(options, current_thread, 0);
    }

    if (note_align)
    {
        google_breakpad::scoped_array<char> scratch(new char[note_align]);
        memset(scratch.get(), 0, note_align);
        if (!writea(options.out_fd, scratch.get(), note_align))
            return 1;
    }

    for (std::map<uint64_t, CrashedProcess::Mapping>::const_iterator iter = crashinfo.mappings.begin(); iter != crashinfo.mappings.end();
         ++iter)
    {
        const CrashedProcess::Mapping & mapping = iter->second;
        if (mapping.data.size())
        {
            if (!writea(options.out_fd, mapping.data.c_str(), mapping.data.size()))
                return 1;
        }
    }

    if (options.out_fd != STDOUT_FILENO)
    {
        close(options.out_fd);
    }

    return 0;
}
}
