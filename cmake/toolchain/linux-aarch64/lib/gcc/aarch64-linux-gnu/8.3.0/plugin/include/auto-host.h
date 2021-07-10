/* auto-host.h.  Generated from config.in by configure.  */
/* config.in.  Generated from configure.ac by autoheader.  */

/* Define if this compiler should be built as the offload target compiler. */
#ifndef USED_FOR_TARGET
/* #undef ACCEL_COMPILER */
#endif


/* Define if building universal (internal helper macro) */
#ifndef USED_FOR_TARGET
/* #undef AC_APPLE_UNIVERSAL_BUILD */
#endif


/* Define to the assembler option to enable compressed debug sections. */
#ifndef USED_FOR_TARGET
#define AS_COMPRESS_DEBUG_OPTION "--compress-debug-sections"
#endif


/* Define to the assembler option to disable compressed debug sections. */
#ifndef USED_FOR_TARGET
#define AS_NO_COMPRESS_DEBUG_OPTION "--nocompress-debug-sections"
#endif


/* Define as the number of bits in a byte, if `limits.h' doesn't. */
#ifndef USED_FOR_TARGET
/* #undef CHAR_BIT */
#endif


/* Define to 0/1 if you want more run-time sanity checks. This one gets a grab
   bag of miscellaneous but relatively cheap checks. */
#ifndef USED_FOR_TARGET
#define CHECKING_P 0
#endif


/* Define 0/1 to force the choice for exception handling model. */
#ifndef USED_FOR_TARGET
/* #undef CONFIG_SJLJ_EXCEPTIONS */
#endif


/* Define to enable the use of a default assembler. */
#ifndef USED_FOR_TARGET
/* #undef DEFAULT_ASSEMBLER */
#endif


/* Define to enable the use of a default linker. */
#ifndef USED_FOR_TARGET
/* #undef DEFAULT_LINKER */
#endif


/* Define if you want to use __cxa_atexit, rather than atexit, to register C++
   destructors for local statics and global objects. This is essential for
   fully standards-compliant handling of destructors, but requires
   __cxa_atexit in libc. */
#ifndef USED_FOR_TARGET
#define DEFAULT_USE_CXA_ATEXIT 2
#endif


/* The default for -fdiagnostics-color option */
#ifndef USED_FOR_TARGET
#define DIAGNOSTICS_COLOR_DEFAULT DIAGNOSTICS_COLOR_AUTO
#endif


/* Define if you want assertions enabled. This is a cheap check. */
#ifndef USED_FOR_TARGET
#define ENABLE_ASSERT_CHECKING 1
#endif


/* Define to 1 to specify that we are using the BID decimal floating point
   format instead of DPD */
#ifndef USED_FOR_TARGET
#define ENABLE_DECIMAL_BID_FORMAT 0
#endif


/* Define to 1 to enable decimal float extension to C. */
#ifndef USED_FOR_TARGET
#define ENABLE_DECIMAL_FLOAT 0
#endif


/* Define if your target supports default PIE and it is enabled. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_DEFAULT_PIE */
#endif


/* Define if your target supports default stack protector and it is enabled.
   */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_DEFAULT_SSP */
#endif


/* Define if you want more run-time sanity checks for dataflow. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_DF_CHECKING */
#endif


/* Define to 0/1 if you want extra run-time checking that might affect code
   generation. */
#ifndef USED_FOR_TARGET
#define ENABLE_EXTRA_CHECKING 0
#endif


/* Define to 1 to enable fixed-point arithmetic extension to C. */
#ifndef USED_FOR_TARGET
#define ENABLE_FIXED_POINT 0
#endif


/* Define if you want fold checked that it never destructs its argument. This
   is quite expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_FOLD_CHECKING */
#endif


/* Define if you want the garbage collector to operate in maximally paranoid
   mode, validating the entire heap and collecting garbage at every
   opportunity. This is extremely expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_GC_ALWAYS_COLLECT */
#endif


/* Define if you want the garbage collector to do object poisoning and other
   memory allocation checks. This is quite expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_GC_CHECKING */
#endif


/* Define if you want operations on GIMPLE (the basic data structure of the
   high-level optimizers) to be checked for dynamic type safety at runtime.
   This is moderately expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_GIMPLE_CHECKING */
#endif


/* Define this to enable support for generating HSAIL. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_HSA */
#endif


/* Define if gcc should always pass --build-id to linker. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_LD_BUILDID */
#endif


/* Define to 1 to enable libquadmath support */
#ifndef USED_FOR_TARGET
#define ENABLE_LIBQUADMATH_SUPPORT 1
#endif


/* Define to enable LTO support. */
#ifndef USED_FOR_TARGET
#define ENABLE_LTO 1
#endif


/* Define to 1 if translation of program messages to the user's native
   language is requested. */
#ifndef USED_FOR_TARGET
#define ENABLE_NLS 1
#endif


/* Define this to enable support for offloading. */
#ifndef USED_FOR_TARGET
#define ENABLE_OFFLOADING 0
#endif


/* Define to enable plugin support. */
#ifndef USED_FOR_TARGET
#define ENABLE_PLUGIN 1
#endif


/* Define if you want all operations on RTL (the basic data structure of the
   optimizer and back end) to be checked for dynamic type safety at runtime.
   This is quite expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_RTL_CHECKING */
#endif


/* Define if you want RTL flag accesses to be checked against the RTL codes
   that are supported for each access macro. This is relatively cheap. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_RTL_FLAG_CHECKING */
#endif


/* Define if you want runtime assertions enabled. This is a cheap check. */
#define ENABLE_RUNTIME_CHECKING 1

/* Define if you want all operations on trees (the basic data structure of the
   front ends) to be checked for dynamic type safety at runtime. This is
   moderately expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_TREE_CHECKING */
#endif


/* Define if you want all gimple types to be verified after gimplifiation.
   This is cheap. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_TYPES_CHECKING */
#endif


/* Define to get calls to the valgrind runtime enabled. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_VALGRIND_ANNOTATIONS */
#endif


/* Define if you want to run subprograms and generated programs through
   valgrind (a memory checker). This is extremely expensive. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_VALGRIND_CHECKING */
#endif


/* Define 0/1 if vtable verification feature is enabled. */
#ifndef USED_FOR_TARGET
#define ENABLE_VTABLE_VERIFY 0
#endif


/* Define to 1 if installation paths should be looked up in the Windows
   Registry. Ignored on non-Windows hosts. */
#ifndef USED_FOR_TARGET
/* #undef ENABLE_WIN32_REGISTRY */
#endif


/* Define to the name of a file containing a list of extra machine modes for
   this architecture. */
#ifndef USED_FOR_TARGET
#define EXTRA_MODES_FILE "config/aarch64/aarch64-modes.def"
#endif


/* Define to enable detailed memory allocation stats gathering. */
#ifndef USED_FOR_TARGET
#define GATHER_STATISTICS 0
#endif


/* Define to 1 if `TIOCGWINSZ' requires <sys/ioctl.h>. */
#ifndef USED_FOR_TARGET
#define GWINSZ_IN_SYS_IOCTL 1
#endif


/* mcontext_t fields start with __ */
#ifndef USED_FOR_TARGET
/* #undef HAS_MCONTEXT_T_UNDERSCORES */
#endif


/* Define if your assembler supports architecture modifiers. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_ARCHITECTURE_MODIFIERS */
#endif


/* Define if your avr assembler supports -mgcc-isr option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_AVR_MGCCISR_OPTION */
#endif


/* Define if your avr assembler supports --mlink-relax option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_AVR_MLINK_RELAX_OPTION */
#endif


/* Define if your avr assembler supports -mrmw option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_AVR_MRMW_OPTION */
#endif


/* Define if your assembler supports cmpb. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_CMPB */
#endif


/* Define to the level of your assembler's compressed debug section support.
   */
#ifndef USED_FOR_TARGET
#define HAVE_AS_COMPRESS_DEBUG 2
#endif


/* Define if your assembler supports the DCI/ICI instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_DCI */
#endif


/* Define if your assembler supports the --debug-prefix-map option. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_DEBUG_PREFIX_MAP 1
#endif


/* Define if your assembler supports DFP instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_DFP */
#endif


/* Define if your assembler supports .module. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_DOT_MODULE */
#endif


/* Define if your assembler supports DSPR1 mult. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_DSPR1_MULT */
#endif


/* Define if your assembler supports .dtprelword. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_DTPRELWORD */
#endif


/* Define if your assembler supports dwarf2 .file/.loc directives, and
   preserves file table indices exactly as given. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_DWARF2_DEBUG_LINE 1
#endif


/* Define if your assembler supports views in dwarf2 .loc directives. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_DWARF2_DEBUG_VIEW 1
#endif


/* Define if your assembler supports the R_PPC64_ENTRY relocation. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_ENTRY_MARKERS */
#endif


/* Define if your assembler supports explicit relocations. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_EXPLICIT_RELOCS */
#endif


/* Define if your assembler supports FMAF, HPC, and VIS 3.0 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_FMAF_HPC_VIS3 */
#endif


/* Define if your assembler supports fprnd. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_FPRND */
#endif


/* Define if your assembler supports the --gdwarf2 option. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_GDWARF2_DEBUG_FLAG 1
#endif


/* Define if your assembler supports .gnu_attribute. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_GNU_ATTRIBUTE */
#endif


/* Define true if the assembler supports '.long foo@GOTOFF'. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_GOTOFF_IN_DATA */
#endif


/* Define if your assembler supports the --gstabs option. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_GSTABS_DEBUG_FLAG 1
#endif


/* Define if your assembler supports the Sun syntax for cmov. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_CMOV_SUN_SYNTAX */
#endif


/* Define if your assembler supports the subtraction of symbols in different
   sections. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_DIFF_SECT_DELTA */
#endif


/* Define if your assembler supports the ffreep mnemonic. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_FFREEP */
#endif


/* Define if your assembler uses fildq and fistq mnemonics. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_FILDQ */
#endif


/* Define if your assembler uses filds and fists mnemonics. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_FILDS */
#endif


/* Define 0/1 if your assembler and linker support @GOT. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_GOT32X */
#endif


/* Define if your assembler supports HLE prefixes. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_HLE */
#endif


/* Define if your assembler supports interunit movq mnemonic. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_INTERUNIT_MOVQ */
#endif


/* Define if your assembler supports the .quad directive. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_QUAD */
#endif


/* Define if the assembler supports 'rep <insn>, lock <insn>'. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_REP_LOCK_PREFIX */
#endif


/* Define if your assembler supports the sahf mnemonic in 64bit mode. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_SAHF */
#endif


/* Define if your assembler supports the swap suffix. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_SWAP */
#endif


/* Define if your assembler and linker support @tlsgdplt. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_TLSGDPLT */
#endif


/* Define to 1 if your assembler and linker support @tlsldm. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_TLSLDM */
#endif


/* Define to 1 if your assembler and linker support @tlsldmplt. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_TLSLDMPLT */
#endif


/* Define 0/1 if your assembler and linker support calling ___tls_get_addr via
   GOT. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_TLS_GET_ADDR_GOT */
#endif


/* Define if your assembler supports the 'ud2' mnemonic. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_IX86_UD2 */
#endif


/* Define if your assembler supports the lituse_jsrdirect relocation. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_JSRDIRECT_RELOCS */
#endif


/* Define if your assembler supports .sleb128 and .uleb128. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_LEB128 1
#endif


/* Define if your assembler supports LEON instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_LEON */
#endif


/* Define if the assembler won't complain about a line such as # 0 "" 2. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_LINE_ZERO 1
#endif


/* Define if your assembler supports ltoffx and ldxmov relocations. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_LTOFFX_LDXMOV_RELOCS */
#endif


/* Define if your assembler supports LWSYNC instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_LWSYNC */
#endif


/* Define if your assembler supports the -mabi option. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_MABI_OPTION 1
#endif


/* Define if your assembler supports .machine and .machinemode. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_MACHINE_MACHINEMODE */
#endif


/* Define if your assembler supports mfcr field. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_MFCRF */
#endif


/* Define if your assembler supports mffgpr and mftgpr. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_MFPGPR */
#endif


/* Define if your Mac OS X assembler supports the -mmacos-version-min option.
   */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_MMACOSX_VERSION_MIN_OPTION */
#endif


/* Define if the assembler understands -mnan=. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_NAN */
#endif


/* Define if your assembler supports the -no-mul-bug-abort option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_NO_MUL_BUG_ABORT_OPTION */
#endif


/* Define if the assembler understands -mno-shared. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_NO_SHARED */
#endif


/* Define if your assembler supports offsetable %lo(). */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_OFFSETABLE_LO10 */
#endif


/* Define if your assembler supports popcntb field. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_POPCNTB */
#endif


/* Define if your assembler supports POPCNTD instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_POPCNTD */
#endif


/* Define if your assembler supports POWER8 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_POWER8 */
#endif


/* Define if your assembler supports POWER9 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_POWER9 */
#endif


/* Define if your assembler supports .ref */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_REF */
#endif


/* Define if your assembler supports .register. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_REGISTER_PSEUDO_OP */
#endif


/* Define if your assembler supports R_PPC_REL16 relocs. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_REL16 */
#endif


/* Define if your assembler supports -relax option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_RELAX_OPTION */
#endif


/* Define if your assembler supports relocs needed by -fpic. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_SMALL_PIC_RELOCS 1
#endif


/* Define if your assembler supports SPARC4 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC4 */
#endif


/* Define if your assembler supports SPARC5 and VIS 4.0 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC5_VIS4 */
#endif


/* Define if your assembler supports SPARC6 instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC6 */
#endif


/* Define if your assembler and linker support GOTDATA_OP relocs. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC_GOTDATA_OP */
#endif


/* Define if your assembler and linker support unaligned PC relative relocs.
   */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC_UA_PCREL */
#endif


/* Define if your assembler and linker support unaligned PC relative relocs
   against hidden symbols. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_SPARC_UA_PCREL_HIDDEN */
#endif


/* Define if your assembler supports .stabs. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_STABS_DIRECTIVE 1
#endif


/* Define if your assembler and linker support thread-local storage. */
#ifndef USED_FOR_TARGET
#define HAVE_AS_TLS 1
#endif


/* Define if your assembler supports arg info for __tls_get_addr. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_TLS_MARKERS */
#endif


/* Define if your assembler supports VSX instructions. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_VSX */
#endif


/* Define if your assembler supports -xbrace_comment option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_AS_XBRACE_COMMENT_OPTION */
#endif


/* Define to 1 if you have the `atoq' function. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_ATOQ */
#endif


/* Define to 1 if you have the `clearerr_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_CLEARERR_UNLOCKED 1
#endif


/* Define to 1 if you have the `clock' function. */
#ifndef USED_FOR_TARGET
#define HAVE_CLOCK 1
#endif


/* Define if <time.h> defines clock_t. */
#ifndef USED_FOR_TARGET
#define HAVE_CLOCK_T 1
#endif


/* Define 0/1 if your assembler and linker support COMDAT groups. */
#ifndef USED_FOR_TARGET
#define HAVE_COMDAT_GROUP 1
#endif


/* Define to 1 if we found a declaration for 'abort', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ABORT 1
#endif


/* Define to 1 if we found a declaration for 'asprintf', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ASPRINTF 1
#endif


/* Define to 1 if we found a declaration for 'atof', otherwise define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ATOF 1
#endif


/* Define to 1 if we found a declaration for 'atol', otherwise define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ATOL 1
#endif


/* Define to 1 if we found a declaration for 'atoll', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ATOLL 1
#endif


/* Define to 1 if you have the declaration of `basename(const char*)', and to
   0 if you don't. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_BASENAME 1
#endif


/* Define to 1 if we found a declaration for 'calloc', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_CALLOC 1
#endif


/* Define to 1 if we found a declaration for 'clearerr_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_CLEARERR_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'clock', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_CLOCK 1
#endif


/* Define to 1 if we found a declaration for 'errno', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_ERRNO 1
#endif


/* Define to 1 if we found a declaration for 'feof_unlocked', otherwise define
   to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FEOF_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'ferror_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FERROR_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fflush_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FFLUSH_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'ffs', otherwise define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FFS 1
#endif


/* Define to 1 if we found a declaration for 'fgetc_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FGETC_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fgets_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FGETS_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fileno_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FILENO_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fprintf_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FPRINTF_UNLOCKED 0
#endif


/* Define to 1 if we found a declaration for 'fputc_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FPUTC_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fputs_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FPUTS_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'fread_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FREAD_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'free', otherwise define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FREE 1
#endif


/* Define to 1 if we found a declaration for 'fwrite_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_FWRITE_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'getchar_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETCHAR_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'getcwd', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETCWD 1
#endif


/* Define to 1 if we found a declaration for 'getc_unlocked', otherwise define
   to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETC_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'getenv', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETENV 1
#endif


/* Define to 1 if we found a declaration for 'getopt', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETOPT 0
#endif


/* Define to 1 if we found a declaration for 'getpagesize', otherwise define
   to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETPAGESIZE 1
#endif


/* Define to 1 if we found a declaration for 'getrlimit', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETRLIMIT 1
#endif


/* Define to 1 if we found a declaration for 'getrusage', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETRUSAGE 1
#endif


/* Define to 1 if we found a declaration for 'getwd', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_GETWD 1
#endif


/* Define to 1 if we found a declaration for 'ldgetname', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_LDGETNAME 0
#endif


/* Define to 1 if we found a declaration for 'madvise', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_MADVISE 1
#endif


/* Define to 1 if we found a declaration for 'malloc', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_MALLOC 1
#endif


/* Define to 1 if we found a declaration for 'putchar_unlocked', otherwise
   define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_PUTCHAR_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'putc_unlocked', otherwise define
   to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_PUTC_UNLOCKED 1
#endif


/* Define to 1 if we found a declaration for 'realloc', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_REALLOC 1
#endif


/* Define to 1 if we found a declaration for 'sbrk', otherwise define to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_SBRK 1
#endif


/* Define to 1 if we found a declaration for 'setenv', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_SETENV 1
#endif


/* Define to 1 if we found a declaration for 'setrlimit', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_SETRLIMIT 1
#endif


/* Define to 1 if we found a declaration for 'sigaltstack', otherwise define
   to 0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_SIGALTSTACK 1
#endif


/* Define to 1 if we found a declaration for 'snprintf', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_SNPRINTF 1
#endif


/* Define to 1 if we found a declaration for 'stpcpy', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STPCPY 1
#endif


/* Define to 1 if we found a declaration for 'strnlen', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRNLEN 1
#endif


/* Define to 1 if we found a declaration for 'strsignal', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRSIGNAL 1
#endif


/* Define to 1 if you have the declaration of `strstr(const char*,const
   char*)', and to 0 if you don't. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRSTR 1
#endif


/* Define to 1 if we found a declaration for 'strtol', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRTOL 1
#endif


/* Define to 1 if we found a declaration for 'strtoll', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRTOLL 1
#endif


/* Define to 1 if we found a declaration for 'strtoul', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRTOUL 1
#endif


/* Define to 1 if we found a declaration for 'strtoull', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRTOULL 1
#endif


/* Define to 1 if we found a declaration for 'strverscmp', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_STRVERSCMP 1
#endif


/* Define to 1 if we found a declaration for 'times', otherwise define to 0.
   */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_TIMES 1
#endif


/* Define to 1 if we found a declaration for 'unsetenv', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_UNSETENV 1
#endif


/* Define to 1 if we found a declaration for 'vasprintf', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_VASPRINTF 1
#endif


/* Define to 1 if we found a declaration for 'vsnprintf', otherwise define to
   0. */
#ifndef USED_FOR_TARGET
#define HAVE_DECL_VSNPRINTF 1
#endif


/* Define to 1 if you have the <direct.h> header file. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_DIRECT_H */
#endif


/* Define to 1 if you have the <dlfcn.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_DLFCN_H 1
#endif


/* Define to 1 if you have the <ext/hash_map> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_EXT_HASH_MAP 1
#endif


/* Define to 1 if you have the <fcntl.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_FCNTL_H 1
#endif


/* Define to 1 if you have the `feof_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FEOF_UNLOCKED 1
#endif


/* Define to 1 if you have the `ferror_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FERROR_UNLOCKED 1
#endif


/* Define to 1 if you have the `fflush_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FFLUSH_UNLOCKED 1
#endif


/* Define to 1 if you have the `fgetc_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FGETC_UNLOCKED 1
#endif


/* Define to 1 if you have the `fgets_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FGETS_UNLOCKED 1
#endif


/* Define to 1 if you have the `fileno_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FILENO_UNLOCKED 1
#endif


/* Define to 1 if you have the `fork' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FORK 1
#endif


/* Define to 1 if you have the `fprintf_unlocked' function. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_FPRINTF_UNLOCKED */
#endif


/* Define to 1 if you have the `fputc_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FPUTC_UNLOCKED 1
#endif


/* Define to 1 if you have the `fputs_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FPUTS_UNLOCKED 1
#endif


/* Define to 1 if you have the `fread_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FREAD_UNLOCKED 1
#endif


/* Define to 1 if you have the <ftw.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_FTW_H 1
#endif


/* Define to 1 if you have the `fwrite_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_FWRITE_UNLOCKED 1
#endif


/* Define if your assembler supports specifying the alignment of objects
   allocated using the GAS .comm command. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_ALIGNED_COMM */
#endif


/* Define if your assembler supports .balign and .p2align. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_BALIGN_AND_P2ALIGN 1
#endif


/* Define 0/1 if your assembler supports CFI directives. */
#define HAVE_GAS_CFI_DIRECTIVE 1

/* Define 0/1 if your assembler supports .cfi_personality. */
#define HAVE_GAS_CFI_PERSONALITY_DIRECTIVE 1

/* Define 0/1 if your assembler supports .cfi_sections. */
#define HAVE_GAS_CFI_SECTIONS_DIRECTIVE 1

/* Define if your assembler supports the .loc discriminator sub-directive. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_DISCRIMINATOR 1
#endif


/* Define if your assembler supports @gnu_unique_object. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_GNU_UNIQUE_OBJECT 1
#endif


/* Define if your assembler and linker support .hidden. */
#define HAVE_GAS_HIDDEN 1

/* Define if your assembler supports .lcomm with an alignment field. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_LCOMM_WITH_ALIGNMENT */
#endif


/* Define if your assembler supports .literal16. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_LITERAL16 */
#endif


/* Define if your assembler supports specifying the maximum number of bytes to
   skip when using the GAS .p2align command. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_MAX_SKIP_P2ALIGN 1
#endif


/* Define if your assembler supports the .set micromips directive */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_MICROMIPS */
#endif


/* Define if your assembler supports .nsubspa comdat option. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_NSUBSPA_COMDAT */
#endif


/* Define if your assembler and linker support 32-bit section relative relocs
   via '.secrel32 label'. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_PE_SECREL32_RELOC */
#endif


/* Define if your assembler supports specifying the section flag e. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GAS_SECTION_EXCLUDE */
#endif


/* Define 0/1 if your assembler supports marking sections with SHF_MERGE flag.
   */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_SHF_MERGE 1
#endif


/* Define if your assembler supports .subsection and .subsection -1 starts
   emitting at the beginning of your section. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_SUBSECTION_ORDERING 1
#endif


/* Define if your assembler supports .weak. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_WEAK 1
#endif


/* Define if your assembler supports .weakref. */
#ifndef USED_FOR_TARGET
#define HAVE_GAS_WEAKREF 1
#endif


/* Define to 1 if you have the `getchar_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_GETCHAR_UNLOCKED 1
#endif


/* Define to 1 if you have the `getc_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_GETC_UNLOCKED 1
#endif


/* Define to 1 if you have the `getrlimit' function. */
#ifndef USED_FOR_TARGET
#define HAVE_GETRLIMIT 1
#endif


/* Define to 1 if you have the `getrusage' function. */
#ifndef USED_FOR_TARGET
#define HAVE_GETRUSAGE 1
#endif


/* Define to 1 if you have the `gettimeofday' function. */
#ifndef USED_FOR_TARGET
#define HAVE_GETTIMEOFDAY 1
#endif


/* Define to 1 if using GNU as. */
#ifndef USED_FOR_TARGET
#define HAVE_GNU_AS 1
#endif


/* Define if your system supports gnu indirect functions. */
#ifndef USED_FOR_TARGET
#define HAVE_GNU_INDIRECT_FUNCTION 1
#endif


/* Define to 1 if using GNU ld. */
#ifndef USED_FOR_TARGET
#define HAVE_GNU_LD 1
#endif


/* Define if the gold linker supports split stack and is available as a
   non-default */
#ifndef USED_FOR_TARGET
/* #undef HAVE_GOLD_NON_DEFAULT_SPLIT_STACK */
#endif


/* Define if you have the iconv() function. */
#ifndef USED_FOR_TARGET
#define HAVE_ICONV 1
#endif


/* Define to 1 if you have the <iconv.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_ICONV_H 1
#endif


/* Define 0/1 if .init_array/.fini_array sections are available and working.
   */
#ifndef USED_FOR_TARGET
#define HAVE_INITFINI_ARRAY_SUPPORT 0
#endif


/* Define to 1 if the system has the type `intmax_t'. */
#ifndef USED_FOR_TARGET
#define HAVE_INTMAX_T 1
#endif


/* Define to 1 if the system has the type `intptr_t'. */
#ifndef USED_FOR_TARGET
#define HAVE_INTPTR_T 1
#endif


/* Define if you have a working <inttypes.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_INTTYPES_H 1
#endif


/* Define to 1 if you have the `kill' function. */
#ifndef USED_FOR_TARGET
#define HAVE_KILL 1
#endif


/* Define if you have <langinfo.h> and nl_langinfo(CODESET). */
#ifndef USED_FOR_TARGET
#define HAVE_LANGINFO_CODESET 1
#endif


/* Define to 1 if you have the <langinfo.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_LANGINFO_H 1
#endif


/* Define if your <locale.h> file defines LC_MESSAGES. */
#ifndef USED_FOR_TARGET
#define HAVE_LC_MESSAGES 1
#endif


/* Define to 1 if you have the <ldfcn.h> header file. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LDFCN_H */
#endif


/* Define if your linker supports --as-needed/--no-as-needed or equivalent
   options. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_AS_NEEDED 1
#endif


/* Define if your default avr linker script for avrxmega3 leaves .rodata in
   flash. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_AVR_AVRXMEGA3_RODATA_IN_FLASH */
#endif


/* Define if your linker supports -z bndplt */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_BNDPLT_SUPPORT */
#endif


/* Define if your linker supports --build-id. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_BUILDID 1
#endif


/* Define if the linker supports clearing hardware capabilities via mapfile.
   */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_CLEARCAP */
#endif


/* Define to the level of your linker's compressed debug section support. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_COMPRESS_DEBUG 3
#endif


/* Define if your linker supports --demangle option. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_DEMANGLE 1
#endif


/* Define 0/1 if your linker supports CIE v3 in .eh_frame. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_EH_FRAME_CIEV3 1
#endif


/* Define if your linker supports .eh_frame_hdr. */
#define HAVE_LD_EH_FRAME_HDR 1

/* Define if your linker supports garbage collection of sections in presence
   of EH frames. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_EH_GC_SECTIONS 1
#endif


/* Define if your linker has buggy garbage collection of sections support when
   .text.startup.foo like sections are used. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_EH_GC_SECTIONS_BUG */
#endif


/* Define if your PowerPC64 linker supports a large TOC. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_LARGE_TOC */
#endif


/* Define if your PowerPC64 linker only needs function descriptor syms. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_NO_DOT_SYMS */
#endif


/* Define if your linker can relax absolute .eh_frame personality pointers
   into PC-relative form. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_PERSONALITY_RELAXATION */
#endif


/* Define if your linker supports PIE option. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_PIE 1
#endif


/* Define 0/1 if your linker supports -pie option with copy reloc. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_PIE_COPYRELOC 0
#endif


/* Define if your PowerPC linker has .gnu.attributes long double support. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_PPC_GNU_ATTR_LONG_DOUBLE */
#endif


/* Define if your linker supports --push-state/--pop-state */
#ifndef USED_FOR_TARGET
#define HAVE_LD_PUSHPOPSTATE_SUPPORT 1
#endif


/* Define if your linker links a mix of read-only and read-write sections into
   a read-write section. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_RO_RW_SECTION_MIXING 1
#endif


/* Define if your linker supports the *_sol2 emulations. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_LD_SOL2_EMULATION */
#endif


/* Define if your linker supports -Bstatic/-Bdynamic or equivalent options. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_STATIC_DYNAMIC 1
#endif


/* Define if your linker supports --sysroot. */
#ifndef USED_FOR_TARGET
#define HAVE_LD_SYSROOT 1
#endif


/* Define to 1 if you have the <limits.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_LIMITS_H 1
#endif


/* Define to 1 if you have the <locale.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_LOCALE_H 1
#endif


/* Define to 1 if the system has the type `long long'. */
#ifndef USED_FOR_TARGET
#define HAVE_LONG_LONG 1
#endif


/* Define to 1 if the system has the type `long long int'. */
#ifndef USED_FOR_TARGET
#define HAVE_LONG_LONG_INT 1
#endif


/* Define to the level of your linker's plugin support. */
#ifndef USED_FOR_TARGET
#define HAVE_LTO_PLUGIN 2
#endif


/* Define to 1 if you have the `madvise' function. */
#ifndef USED_FOR_TARGET
#define HAVE_MADVISE 1
#endif


/* Define to 1 if you have the <malloc.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_MALLOC_H 1
#endif


/* Define to 1 if you have the `mbstowcs' function. */
#ifndef USED_FOR_TARGET
#define HAVE_MBSTOWCS 1
#endif


/* Define if valgrind's memcheck.h header is installed. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_MEMCHECK_H */
#endif


/* Define to 1 if you have the <memory.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_MEMORY_H 1
#endif


/* Define to 1 if you have the `mmap' function. */
#ifndef USED_FOR_TARGET
#define HAVE_MMAP 1
#endif


/* Define if mmap with MAP_ANON(YMOUS) works. */
#ifndef USED_FOR_TARGET
#define HAVE_MMAP_ANON 1
#endif


/* Define if mmap of /dev/zero works. */
#ifndef USED_FOR_TARGET
#define HAVE_MMAP_DEV_ZERO 1
#endif


/* Define if read-only mmap of a plain file works. */
#ifndef USED_FOR_TARGET
#define HAVE_MMAP_FILE 1
#endif


/* Define to 1 if you have the `nl_langinfo' function. */
#ifndef USED_FOR_TARGET
#define HAVE_NL_LANGINFO 1
#endif


/* Define to 1 if you have the `popen' function. */
#ifndef USED_FOR_TARGET
#define HAVE_POPEN 1
#endif


/* Define to 1 if you have the `putchar_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_PUTCHAR_UNLOCKED 1
#endif


/* Define to 1 if you have the `putc_unlocked' function. */
#ifndef USED_FOR_TARGET
#define HAVE_PUTC_UNLOCKED 1
#endif


/* Define to 1 if you have the `setlocale' function. */
#ifndef USED_FOR_TARGET
#define HAVE_SETLOCALE 1
#endif


/* Define to 1 if you have the `setrlimit' function. */
#ifndef USED_FOR_TARGET
#define HAVE_SETRLIMIT 1
#endif


/* Define if the system-provided CRTs are present on Solaris. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_SOLARIS_CRTS */
#endif


/* Define to 1 if you have the <stddef.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_STDDEF_H 1
#endif


/* Define to 1 if you have the <stdint.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_STDINT_H 1
#endif


/* Define to 1 if you have the <stdlib.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_STDLIB_H 1
#endif


/* Define to 1 if you have the <strings.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_STRINGS_H 1
#endif


/* Define to 1 if you have the <string.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_STRING_H 1
#endif


/* Define to 1 if you have the `strsignal' function. */
#ifndef USED_FOR_TARGET
#define HAVE_STRSIGNAL 1
#endif


/* Define if <sys/times.h> defines struct tms. */
#ifndef USED_FOR_TARGET
#define HAVE_STRUCT_TMS 1
#endif


/* Define if <utility> defines std::swap. */
#ifndef USED_FOR_TARGET
#define HAVE_SWAP_IN_UTILITY 1
#endif


/* Define to 1 if you have the `sysconf' function. */
#ifndef USED_FOR_TARGET
#define HAVE_SYSCONF 1
#endif


/* Define to 1 if you have the <sys/file.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_FILE_H 1
#endif


/* Define to 1 if you have the <sys/mman.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_MMAN_H 1
#endif


/* Define to 1 if you have the <sys/param.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_PARAM_H 1
#endif


/* Define to 1 if you have the <sys/resource.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_RESOURCE_H 1
#endif


/* Define if your target C library provides sys/sdt.h */
/* #undef HAVE_SYS_SDT_H */

/* Define to 1 if you have the <sys/stat.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_STAT_H 1
#endif


/* Define to 1 if you have the <sys/times.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_TIMES_H 1
#endif


/* Define to 1 if you have the <sys/time.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_TIME_H 1
#endif


/* Define to 1 if you have the <sys/types.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_TYPES_H 1
#endif


/* Define to 1 if you have <sys/wait.h> that is POSIX.1 compatible. */
#ifndef USED_FOR_TARGET
#define HAVE_SYS_WAIT_H 1
#endif


/* Define to 1 if you have the `times' function. */
#ifndef USED_FOR_TARGET
#define HAVE_TIMES 1
#endif


/* Define to 1 if you have the <time.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_TIME_H 1
#endif


/* Define to 1 if you have the <tr1/unordered_map> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_TR1_UNORDERED_MAP 1
#endif


/* Define to 1 if the system has the type `uintmax_t'. */
#ifndef USED_FOR_TARGET
#define HAVE_UINTMAX_T 1
#endif


/* Define to 1 if the system has the type `uintptr_t'. */
#ifndef USED_FOR_TARGET
#define HAVE_UINTPTR_T 1
#endif


/* Define to 1 if you have the <unistd.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_UNISTD_H 1
#endif


/* Define to 1 if you have the <unordered_map> header file. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_UNORDERED_MAP */
#endif


/* Define to 1 if the system has the type `unsigned long long int'. */
#ifndef USED_FOR_TARGET
#define HAVE_UNSIGNED_LONG_LONG_INT 1
#endif


/* Define if valgrind's valgrind/memcheck.h header is installed. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_VALGRIND_MEMCHECK_H */
#endif


/* Define to 1 if you have the `vfork' function. */
#ifndef USED_FOR_TARGET
#define HAVE_VFORK 1
#endif


/* Define to 1 if you have the <vfork.h> header file. */
#ifndef USED_FOR_TARGET
/* #undef HAVE_VFORK_H */
#endif


/* Define to 1 if you have the <wchar.h> header file. */
#ifndef USED_FOR_TARGET
#define HAVE_WCHAR_H 1
#endif


/* Define to 1 if you have the `wcswidth' function. */
#ifndef USED_FOR_TARGET
#define HAVE_WCSWIDTH 1
#endif


/* Define to 1 if `fork' works. */
#ifndef USED_FOR_TARGET
#define HAVE_WORKING_FORK 1
#endif


/* Define this macro if mbstowcs does not crash when its first argument is
   NULL. */
#ifndef USED_FOR_TARGET
#define HAVE_WORKING_MBSTOWCS 1
#endif


/* Define to 1 if `vfork' works. */
#ifndef USED_FOR_TARGET
#define HAVE_WORKING_VFORK 1
#endif


/* Define if your assembler supports AIX debug frame section label reference.
   */
#ifndef USED_FOR_TARGET
/* #undef HAVE_XCOFF_DWARF_EXTRAS */
#endif


/* Define if isl is in use. */
#ifndef USED_FOR_TARGET
#define HAVE_isl 1
#endif


/* Define if F_SETLKW supported by fcntl. */
#ifndef USED_FOR_TARGET
#define HOST_HAS_F_SETLKW 1
#endif


/* Define as const if the declaration of iconv() needs const. */
#ifndef USED_FOR_TARGET
#define ICONV_CONST 
#endif


/* Define if int64_t uses long as underlying type. */
#ifndef USED_FOR_TARGET
#define INT64_T_IS_LONG 1
#endif


/* Define to 1 if ld64 supports '-export_dynamic'. */
#ifndef USED_FOR_TARGET
/* #undef LD64_HAS_EXPORT_DYNAMIC */
#endif


/* Define to ld64 version. */
#ifndef USED_FOR_TARGET
/* #undef LD64_VERSION */
#endif


/* Define to the linker option to ignore unused dependencies. */
#ifndef USED_FOR_TARGET
#define LD_AS_NEEDED_OPTION "--as-needed"
#endif


/* Define to the linker option to enable compressed debug sections. */
#ifndef USED_FOR_TARGET
#define LD_COMPRESS_DEBUG_OPTION "--compress-debug-sections"
#endif


/* Define to the linker option to enable use of shared objects. */
#ifndef USED_FOR_TARGET
#define LD_DYNAMIC_OPTION "-Bdynamic"
#endif


/* Define to the linker option to keep unused dependencies. */
#ifndef USED_FOR_TARGET
#define LD_NO_AS_NEEDED_OPTION "--no-as-needed"
#endif


/* Define to the linker option to disable use of shared objects. */
#ifndef USED_FOR_TARGET
#define LD_STATIC_OPTION "-Bstatic"
#endif


/* The linker hash style */
#ifndef USED_FOR_TARGET
/* #undef LINKER_HASH_STYLE */
#endif


/* Define to the name of the LTO plugin DSO that must be passed to the
   linker's -plugin=LIB option. */
#ifndef USED_FOR_TARGET
#define LTOPLUGINSONAME "liblto_plugin.so"
#endif


/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#ifndef USED_FOR_TARGET
#define LT_OBJDIR ".libs/"
#endif


/* Value to set mingw's _dowildcard to. */
#ifndef USED_FOR_TARGET
/* #undef MINGW_DOWILDCARD */
#endif


/* Define if host mkdir takes a single argument. */
#ifndef USED_FOR_TARGET
/* #undef MKDIR_TAKES_ONE_ARG */
#endif


/* Define to offload targets, separated by commas. */
#ifndef USED_FOR_TARGET
#define OFFLOAD_TARGETS ""
#endif


/* Define to the address where bug reports for this package should be sent. */
#ifndef USED_FOR_TARGET
#define PACKAGE_BUGREPORT ""
#endif


/* Define to the full name of this package. */
#ifndef USED_FOR_TARGET
#define PACKAGE_NAME ""
#endif


/* Define to the full name and version of this package. */
#ifndef USED_FOR_TARGET
#define PACKAGE_STRING ""
#endif


/* Define to the one symbol short name of this package. */
#ifndef USED_FOR_TARGET
#define PACKAGE_TARNAME ""
#endif


/* Define to the home page for this package. */
#ifndef USED_FOR_TARGET
#define PACKAGE_URL ""
#endif


/* Define to the version of this package. */
#ifndef USED_FOR_TARGET
#define PACKAGE_VERSION ""
#endif


/* Specify plugin linker */
#ifndef USED_FOR_TARGET
#define PLUGIN_LD_SUFFIX "ld"
#endif


/* Define to .TOC. alignment forced by your linker. */
#ifndef USED_FOR_TARGET
/* #undef POWERPC64_TOC_POINTER_ALIGNMENT */
#endif


/* Define to PREFIX/include if cpp should also search that directory. */
#ifndef USED_FOR_TARGET
/* #undef PREFIX_INCLUDE_DIR */
#endif


/* The size of `int', as computed by sizeof. */
#ifndef USED_FOR_TARGET
#define SIZEOF_INT 4
#endif


/* The size of `long', as computed by sizeof. */
#ifndef USED_FOR_TARGET
#define SIZEOF_LONG 8
#endif


/* The size of `long long', as computed by sizeof. */
#ifndef USED_FOR_TARGET
#define SIZEOF_LONG_LONG 8
#endif


/* The size of `short', as computed by sizeof. */
#ifndef USED_FOR_TARGET
#define SIZEOF_SHORT 2
#endif


/* The size of `void *', as computed by sizeof. */
#ifndef USED_FOR_TARGET
#define SIZEOF_VOID_P 8
#endif


/* Define to 1 if you have the ANSI C header files. */
#ifndef USED_FOR_TARGET
#define STDC_HEADERS 1
#endif


/* Define if you can safely include both <string.h> and <strings.h>. */
#ifndef USED_FOR_TARGET
#define STRING_WITH_STRINGS 1
#endif


/* Define if TFmode long double should be the default */
#ifndef USED_FOR_TARGET
/* #undef TARGET_DEFAULT_LONG_DOUBLE_128 */
#endif


/* Define if your target C library provides the `dl_iterate_phdr' function. */
/* #undef TARGET_DL_ITERATE_PHDR */

/* GNU C Library major version number used on the target, or 0. */
#ifndef USED_FOR_TARGET
#define TARGET_GLIBC_MAJOR 2
#endif


/* GNU C Library minor version number used on the target, or 0. */
#ifndef USED_FOR_TARGET
#define TARGET_GLIBC_MINOR 28
#endif


/* Define if your target C Library provides the AT_HWCAP value in the TCB */
#ifndef USED_FOR_TARGET
/* #undef TARGET_LIBC_PROVIDES_HWCAP_IN_TCB */
#endif


/* Define if your target C library provides stack protector support */
#ifndef USED_FOR_TARGET
#define TARGET_LIBC_PROVIDES_SSP 1
#endif


/* Define to 1 if you can safely include both <sys/time.h> and <time.h>. */
#ifndef USED_FOR_TARGET
#define TIME_WITH_SYS_TIME 1
#endif


/* Define to the flag used to mark TLS sections if the default (`T') doesn't
   work. */
#ifndef USED_FOR_TARGET
/* #undef TLS_SECTION_ASM_FLAG */
#endif


/* Define if your assembler mis-optimizes .eh_frame data. */
#ifndef USED_FOR_TARGET
/* #undef USE_AS_TRADITIONAL_FORMAT */
#endif


/* Define if you want to generate code by default that assumes that the Cygwin
   DLL exports wrappers to support libstdc++ function replacement. */
#ifndef USED_FOR_TARGET
/* #undef USE_CYGWIN_LIBSTDCXX_WRAPPERS */
#endif


/* Define 0/1 if your linker supports hidden thunks in linkonce sections. */
#ifndef USED_FOR_TARGET
/* #undef USE_HIDDEN_LINKONCE */
#endif


/* Define to 1 if the 'long long' type is wider than 'long' but still
   efficiently supported by the host hardware. */
#ifndef USED_FOR_TARGET
/* #undef USE_LONG_LONG_FOR_WIDEST_FAST_INT */
#endif


/* Define if we should use leading underscore on 64 bit mingw targets */
#ifndef USED_FOR_TARGET
/* #undef USE_MINGW64_LEADING_UNDERSCORES */
#endif


/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif


/* Define to be the last component of the Windows registry key under which to
   look for installation paths. The full key used will be
   HKEY_LOCAL_MACHINE/SOFTWARE/Free Software Foundation/{WIN32_REGISTRY_KEY}.
   The default is the GCC version number. */
#ifndef USED_FOR_TARGET
/* #undef WIN32_REGISTRY_KEY */
#endif


/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Number of bits in a file offset, on hosts where this is settable. */
#ifndef USED_FOR_TARGET
/* #undef _FILE_OFFSET_BITS */
#endif


/* Define for large files, on AIX-style hosts. */
#ifndef USED_FOR_TARGET
/* #undef _LARGE_FILES */
#endif


/* Define to 1 if on MINIX. */
#ifndef USED_FOR_TARGET
/* #undef _MINIX */
#endif


/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
#ifndef USED_FOR_TARGET
/* #undef _POSIX_1_SOURCE */
#endif


/* Define to 1 if you need to in order for `stat' and other things to work. */
#ifndef USED_FOR_TARGET
/* #undef _POSIX_SOURCE */
#endif


/* Define for Solaris 2.5.1 so the uint32_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
#ifndef USED_FOR_TARGET
/* #undef _UINT32_T */
#endif


/* Define for Solaris 2.5.1 so the uint64_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
#ifndef USED_FOR_TARGET
/* #undef _UINT64_T */
#endif


/* Define for Solaris 2.5.1 so the uint8_t typedef from <sys/synch.h>,
   <pthread.h>, or <semaphore.h> is not used. If the typedef were allowed, the
   #define below would cause a syntax error. */
#ifndef USED_FOR_TARGET
/* #undef _UINT8_T */
#endif


/* Define to `char *' if <sys/types.h> does not define. */
#ifndef USED_FOR_TARGET
/* #undef caddr_t */
#endif


/* Define to `__inline__' or `__inline' if that's what the C compiler
   calls it, or to nothing if 'inline' is not supported under any name.  */
#ifndef __cplusplus
/* #undef inline */
#endif

/* Define to the type of a signed integer type of width exactly 16 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef int16_t */
#endif


/* Define to the type of a signed integer type of width exactly 32 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef int32_t */
#endif


/* Define to the type of a signed integer type of width exactly 64 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef int64_t */
#endif


/* Define to the type of a signed integer type of width exactly 8 bits if such
   a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef int8_t */
#endif


/* Define to the widest signed integer type if <stdint.h> and <inttypes.h> do
   not define. */
#ifndef USED_FOR_TARGET
/* #undef intmax_t */
#endif


/* Define to the type of a signed integer type wide enough to hold a pointer,
   if such a type exists, and if the system does not define it. */
#ifndef USED_FOR_TARGET
/* #undef intptr_t */
#endif


/* Define to `int' if <sys/types.h> does not define. */
#ifndef USED_FOR_TARGET
/* #undef pid_t */
#endif


/* Define to `long' if <sys/resource.h> doesn't define. */
#ifndef USED_FOR_TARGET
/* #undef rlim_t */
#endif


/* Define to `int' if <sys/types.h> does not define. */
#ifndef USED_FOR_TARGET
/* #undef ssize_t */
#endif


/* Define to the type of an unsigned integer type of width exactly 16 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef uint16_t */
#endif


/* Define to the type of an unsigned integer type of width exactly 32 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef uint32_t */
#endif


/* Define to the type of an unsigned integer type of width exactly 64 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef uint64_t */
#endif


/* Define to the type of an unsigned integer type of width exactly 8 bits if
   such a type exists and the standard includes do not define it. */
#ifndef USED_FOR_TARGET
/* #undef uint8_t */
#endif


/* Define to the widest unsigned integer type if <stdint.h> and <inttypes.h>
   do not define. */
#ifndef USED_FOR_TARGET
/* #undef uintmax_t */
#endif


/* Define to the type of an unsigned integer type wide enough to hold a
   pointer, if such a type exists, and if the system does not define it. */
#ifndef USED_FOR_TARGET
/* #undef uintptr_t */
#endif


/* Define as `fork' if `vfork' does not work. */
#ifndef USED_FOR_TARGET
/* #undef vfork */
#endif

