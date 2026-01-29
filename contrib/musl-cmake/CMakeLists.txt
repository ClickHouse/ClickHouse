# Build musl libc from source
# Excludes math functions (provided by LLVM-libc)

set(MUSL_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl")

# ============================================================================
# Architecture Detection
# ============================================================================

if (ARCH_AMD64)
    set(MUSL_ARCH "x86_64")
elseif (ARCH_AARCH64)
    set(MUSL_ARCH "aarch64")
elseif (ARCH_PPC64LE)
    set(MUSL_ARCH "powerpc64")
elseif (ARCH_S390X)
    set(MUSL_ARCH "s390x")
elseif (ARCH_RISCV64)
    set(MUSL_ARCH "riscv64")
elseif (ARCH_LOONGARCH64)
    set(MUSL_ARCH "loongarch64")
else()
    message(FATAL_ERROR "Unsupported architecture for musl: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

message(STATUS "Building musl libc for architecture: ${MUSL_ARCH}")

# ============================================================================
# Generate Required Headers
# ============================================================================

# Create output directory for generated headers
set(MUSL_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")
file(MAKE_DIRECTORY "${MUSL_GENERATED_DIR}/bits")

# Generate bits/alltypes.h
# This combines arch-specific and generic type definitions using sed script
add_custom_command(
    OUTPUT "${MUSL_GENERATED_DIR}/bits/alltypes.h"
    COMMAND sed -f "${MUSL_SOURCE_DIR}/tools/mkalltypes.sed"
            "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}/bits/alltypes.h.in"
            "${MUSL_SOURCE_DIR}/include/alltypes.h.in"
            > "${MUSL_GENERATED_DIR}/bits/alltypes.h"
    DEPENDS 
        "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}/bits/alltypes.h.in"
        "${MUSL_SOURCE_DIR}/include/alltypes.h.in"
        "${MUSL_SOURCE_DIR}/tools/mkalltypes.sed"
    COMMENT "Generating bits/alltypes.h for ${MUSL_ARCH}"
    VERBATIM
)

# Generate bits/syscall.h
# Copy syscall numbers and generate SYS_* aliases
add_custom_command(
    OUTPUT "${MUSL_GENERATED_DIR}/bits/syscall.h"
    COMMAND cp "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}/bits/syscall.h.in" 
               "${MUSL_GENERATED_DIR}/bits/syscall.h"
    COMMAND sed -n -e "s/__NR_/SYS_/p" 
                "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}/bits/syscall.h.in" 
                >> "${MUSL_GENERATED_DIR}/bits/syscall.h"
    DEPENDS "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}/bits/syscall.h.in"
    COMMENT "Generating bits/syscall.h for ${MUSL_ARCH}"
    VERBATIM
)

# Generate version.h
execute_process(
    COMMAND sh "${MUSL_SOURCE_DIR}/tools/version.sh"
    WORKING_DIRECTORY "${MUSL_SOURCE_DIR}"
    OUTPUT_VARIABLE MUSL_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(MUSL_VERSION_HEADER "${CMAKE_CURRENT_BINARY_DIR}/version.h")
file(WRITE "${MUSL_VERSION_HEADER}" "#define VERSION \"${MUSL_VERSION}\"\n")

# Create a custom target for all generated headers
add_custom_target(musl-headers
    DEPENDS 
        "${MUSL_GENERATED_DIR}/bits/alltypes.h"
        "${MUSL_GENERATED_DIR}/bits/syscall.h"
        "${MUSL_VERSION_HEADER}"
)

# ============================================================================
# Collect Source Files
# ============================================================================

# List of ALL architecture subdirectories in musl src/
# These must be excluded from base source collection to avoid
# compiling arch-specific code for the wrong architecture
set(MUSL_ALL_ARCHS
    aarch64 arm i386 loongarch64 m68k microblaze mips mips64 mipsn32
    or1k powerpc powerpc64 riscv32 riscv64 s390x sh x32 x86_64
)

# Get ALL C sources recursively, then filter out what we don't want
file(GLOB_RECURSE MUSL_SOURCES_ALL "${MUSL_SOURCE_DIR}/src/*.c")

# Exclude architecture-specific sources for ALL archs first
# We'll add back the correct arch-specific sources separately
foreach(arch ${MUSL_ALL_ARCHS})
    list(FILTER MUSL_SOURCES_ALL EXCLUDE REGEX ".*/src/[^/]+/${arch}/.*")
endforeach()

# Now we have generic sources only. Create the base list.
set(MUSL_SOURCES_BASE ${MUSL_SOURCES_ALL})

# ============================================================================
# Handle Arch Override Sources
# ============================================================================
# When an arch-specific implementation exists (either .c or .s), we must
# exclude the generic .c version. This is how musl's build system works.
#
# Example: src/thread/__set_thread_area.c should be excluded when
#          src/thread/x86_64/__set_thread_area.s exists

# Get list of arch-specific source filenames (basenames without extension)
file(GLOB_RECURSE MUSL_ARCH_OVERRIDES
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.c"
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.s"
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.S"
)

# For each arch-specific file, exclude the corresponding generic file
foreach(arch_file ${MUSL_ARCH_OVERRIDES})
    # Extract filename (e.g., "__set_thread_area.s" -> "__set_thread_area")
    get_filename_component(arch_filename ${arch_file} NAME_WE)
    # Extract the parent directory category (e.g., "thread", "signal", etc.)
    get_filename_component(arch_subdir ${arch_file} DIRECTORY)  # .../src/thread/x86_64
    get_filename_component(arch_subdir ${arch_subdir} DIRECTORY) # .../src/thread
    get_filename_component(category ${arch_subdir} NAME)          # thread
    
    # Build pattern to match generic file: .../src/<category>/<basename>.c
    set(generic_pattern ".*/src/${category}/${arch_filename}\\.c$")
    
    # Count before filtering
    list(LENGTH MUSL_SOURCES_BASE _before_count)
    
    # Remove the generic version if it exists
    list(FILTER MUSL_SOURCES_BASE EXCLUDE REGEX "${generic_pattern}")
    
    # Debug: check if something was excluded
    list(LENGTH MUSL_SOURCES_BASE _after_count)
    if(NOT _before_count EQUAL _after_count)
        message(STATUS "Musl: excluded generic ${category}/${arch_filename}.c (arch override exists)")
    endif()
endforeach()

# ============================================================================
# Handle Math Functions - Selective Inclusion
# ============================================================================
# LLVM-libc provides most float (f suffix) and double functions, BUT:
# - It does NOT provide long double functions (*l suffix) like powl, expl, logl, sinl, cosl, etc.
# - It does NOT provide some special functions: tgamma, lgamma, erf (double), etc.
# 
# Strategy: Exclude ONLY the math functions that LLVM-libc provides, keep the rest.

# List of math function BASENAMES that LLVM-libc provides (we'll exclude these from musl)
# These are functions where LLVM-libc has working implementations
set(LLVM_LIBC_MATH_FUNCTIONS
    # Basic trigonometry - float
    sinf cosf tanf asinf acosf atanf atan2f
    sinhf coshf tanhf asinhf acoshf atanhf
    # Basic trigonometry - double  
    sin cos tan asin acos atan atan2
    sinh cosh tanh asinh acosh atanh
    # Exponential/logarithmic - float
    expf exp2f exp10f expm1f logf log2f log10f log1pf powf
    # Exponential/logarithmic - double
    exp exp2 exp10 expm1 log log2 log10 log1p pow
    # Root/power - float
    sqrtf cbrtf hypotf
    # Root/power - double
    sqrt cbrt hypot
    # Rounding - float
    ceilf floorf truncf roundf rintf nearbyintf
    # Rounding - double
    ceil floor trunc round rint nearbyint
    # Rounding - long double (these ARE in LLVM-libc)
    ceill floorl truncl roundl rintl nearbyintl
    # Other basic math - float
    fabsf fmodf remainderf remquof copysignf
    fdimf fmaxf fminf fmaf
    # Other basic math - double
    fabs fmod remainder remquo copysign
    fdim fmax fmin fma
    # Other basic - long double (some ARE in LLVM-libc)
    fabsl fmodl remainderl remquol copysignl
    fdiml fmaxl fminl
    # Classification/conversion - float
    frexpf ldexpf modff scalbnf ilogbf logbf
    # Classification/conversion - double
    frexp ldexp modf scalbn ilogb logb
    # Classification/conversion - long double
    frexpl ldexpl modfl scalbnl ilogbl logbl
    # Integer rounding - float
    llrintf lrintf llroundf lroundf
    # Integer rounding - double
    llrint lrint llround lround
    # Integer rounding - long double
    llrintl lrintl llroundl lroundl
    # Next functions
    nextafterf nexttowardf nextafter nexttoward
    nextafterl nexttowardl
    # NaN
    nan nanf nanl
    # Special - float only in LLVM-libc
    sincosf cospif sinpif erff
)

# Create regex pattern to match musl math files we want to EXCLUDE
# (because LLVM-libc provides them)
set(MUSL_MATH_EXCLUDE_PATTERN "")
foreach(func ${LLVM_LIBC_MATH_FUNCTIONS})
    if(MUSL_MATH_EXCLUDE_PATTERN)
        set(MUSL_MATH_EXCLUDE_PATTERN "${MUSL_MATH_EXCLUDE_PATTERN}|")
    endif()
    set(MUSL_MATH_EXCLUDE_PATTERN "${MUSL_MATH_EXCLUDE_PATTERN}/${func}\\.c$")
endforeach()

# Apply the exclusion
list(FILTER MUSL_SOURCES_BASE EXCLUDE REGEX ".*/math/(${MUSL_MATH_EXCLUDE_PATTERN})")

# Also exclude __math_* internal files that might conflict
# But keep __invtrigl.c, __polevll.c, __tanl.c etc. needed by long double functions
# list(FILTER MUSL_SOURCES_BASE EXCLUDE REGEX ".*/math/__.*\\.c$")

message(STATUS "Musl math: keeping long double and special functions not in LLVM-libc")

# Get architecture-specific C sources for TARGET architecture only
# Use GLOB_RECURSE to catch nested arch dirs
file(GLOB_RECURSE MUSL_ARCH_C_SOURCES 
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.c"
)

# Apply the same math exclusion pattern to arch-specific sources
list(FILTER MUSL_ARCH_C_SOURCES EXCLUDE REGEX ".*/math/(${MUSL_MATH_EXCLUDE_PATTERN})")

# Get architecture-specific assembly sources for TARGET architecture only
file(GLOB_RECURSE MUSL_ARCH_ASM_SOURCES 
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.s"
    "${MUSL_SOURCE_DIR}/src/*/${MUSL_ARCH}/*.S"
)

# Apply the same math exclusion pattern to arch-specific ASM
list(FILTER MUSL_ARCH_ASM_SOURCES EXCLUDE REGEX ".*/math/(${MUSL_MATH_EXCLUDE_PATTERN})")

# Get ldso (dynamic linker) generic sources
file(GLOB MUSL_LDSO_SOURCES 
    "${MUSL_SOURCE_DIR}/ldso/*.c"
)

# Get ldso architecture-specific sources
file(GLOB MUSL_LDSO_ARCH_SOURCES
    "${MUSL_SOURCE_DIR}/ldso/${MUSL_ARCH}/*.c"
    "${MUSL_SOURCE_DIR}/ldso/${MUSL_ARCH}/*.s"
    "${MUSL_SOURCE_DIR}/ldso/${MUSL_ARCH}/*.S"
)

# Get time32 compatibility sources (for 32-bit time_t compatibility)
file(GLOB MUSL_COMPAT_SOURCES 
    "${MUSL_SOURCE_DIR}/compat/time32/*.c"
)

# Enable assembly language
enable_language(ASM)

# ============================================================================
# Separate ASM sources from C sources
# ============================================================================

# Collect all ASM sources
set(MUSL_ALL_ASM_SOURCES ${MUSL_ARCH_ASM_SOURCES})

# Filter ldso arch sources to separate ASM from C
set(MUSL_LDSO_ARCH_C_SOURCES "")
set(MUSL_LDSO_ARCH_ASM "")
foreach(src ${MUSL_LDSO_ARCH_SOURCES})
    if(src MATCHES "\\.(s|S)$")
        list(APPEND MUSL_LDSO_ARCH_ASM ${src})
    else()
        list(APPEND MUSL_LDSO_ARCH_C_SOURCES ${src})
    endif()
endforeach()
list(APPEND MUSL_ALL_ASM_SOURCES ${MUSL_LDSO_ARCH_ASM})

# Combine C sources only (no ASM)
set(MUSL_C_SOURCES 
    ${MUSL_SOURCES_BASE}
    ${MUSL_ARCH_C_SOURCES}
    ${MUSL_LDSO_SOURCES}
    ${MUSL_LDSO_ARCH_C_SOURCES}
    ${MUSL_COMPAT_SOURCES}
)

# Remove duplicates
list(REMOVE_DUPLICATES MUSL_C_SOURCES)
list(REMOVE_DUPLICATES MUSL_ALL_ASM_SOURCES)

list(LENGTH MUSL_C_SOURCES MUSL_C_SOURCE_COUNT)
list(LENGTH MUSL_ALL_ASM_SOURCES MUSL_ASM_SOURCE_COUNT)
message(STATUS "Building musl with ${MUSL_C_SOURCE_COUNT} C sources and ${MUSL_ASM_SOURCE_COUNT} ASM sources (partial math - long double and special functions)")

# ============================================================================
# Build Main musl Library (C + ASM sources combined)
# ============================================================================

# Combine ALL sources (C and ASM) into a single static library
# This avoids circular dependency issues with OBJECT libraries
set(MUSL_ALL_SOURCES ${MUSL_C_SOURCES} ${MUSL_ALL_ASM_SOURCES})

add_library(musl STATIC ${MUSL_ALL_SOURCES})
set_target_properties(musl PROPERTIES FOLDER "contrib/musl-cmake")

# Set ASM language explicitly for assembly files
foreach(asm_file ${MUSL_ALL_ASM_SOURCES})
    set_source_files_properties(${asm_file} PROPERTIES 
        LANGUAGE ASM
        # Per-file compile flags for ASM:
        # -Qunused-arguments prevents errors about C flags being unused by assembler
        # -Wa,--noexecstack marks stack as non-executable
        COMPILE_FLAGS "-Qunused-arguments -Wa,--noexecstack"
    )
endforeach()

# Ensure generated headers are created first
add_dependencies(musl musl-headers)

# ============================================================================
# Include Directories
# ============================================================================

# CRITICAL: For building musl itself, the include order MUST be:
#   1. include-override      - Wrapper headers that force correct includes
#   2. arch/${MUSL_ARCH}     - arch-specific headers
#   3. arch/generic          - generic arch fallbacks
#   4. src/include           - internal overrides (features.h with 'hidden' macro)
#   5. src/internal          - internal headers
#   6. generated dir         - bits/alltypes.h, bits/syscall.h
#   7. build dir             - version.h
#
# The problem: sigaction.c includes "ksigaction.h" with quotes. On x86_64,
# arch/x86_64/ksigaction.h defines `__restore` as an alias to `__restore_rt`,
# while src/internal/ksigaction.h declares them as separate symbols.
#
# CMake deduplication issue: If arch dirs are in both PRIVATE and PUBLIC,
# CMake keeps only one copy. To work around this, we use an override header
# in include-override/ that explicitly includes the correct arch-specific file.

# Override directory with wrapper headers - comes FIRST
set(MUSL_OVERRIDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include-override")

# Private includes for building musl
target_include_directories(musl BEFORE PRIVATE
    "${MUSL_OVERRIDE_DIR}"
    "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}"
    "${MUSL_SOURCE_DIR}/arch/generic"
    "${MUSL_SOURCE_DIR}/src/include"
    "${MUSL_SOURCE_DIR}/src/internal"
    "${MUSL_GENERATED_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}"
)

# Public includes for external code that links to musl
# These provide bits/*.h headers to dependents
target_include_directories(musl SYSTEM PUBLIC
    "${MUSL_GENERATED_DIR}"
    "${MUSL_SOURCE_DIR}/include"
    "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}"
    "${MUSL_SOURCE_DIR}/arch/generic"
)

# ============================================================================
# Compiler Flags
# ============================================================================

target_compile_options(musl PRIVATE
    # Standard C99
    -std=c99
    
    # Freestanding environment (no hosted libc)
    -ffreestanding
    
    # Don't use compiler builtins (we provide them)
    -fno-builtin
    
    # No stack protector (we're the libc!)
    -fno-stack-protector
    
    # Performance: omit frame pointers
    -fomit-frame-pointer
    
    # CRITICAL for unwinding (from clickhouse-configure script)
    # These flags tell the compiler to generate unwind tables
    # Required for C++ exceptions and stack traces
    -funwind-tables
    -fasynchronous-unwind-tables
    
    # Optimization for size and performance
    -ffunction-sections
    -fdata-sections
    
    # Warnings as errors for common mistakes
    -Werror=implicit-function-declaration
    -Werror=implicit-int
    -Werror=pointer-sign
    -Werror=pointer-arith
)

# Suppress some expected warnings in musl code
target_compile_options(musl PRIVATE
    -Wno-unused-function
    -Wno-unused-variable
    -Wno-dangling-else
    -Wno-missing-braces
    -Wno-parentheses
    -Wno-shift-op-parentheses
    -Wno-string-plus-int
    -Wno-unknown-pragmas
    -Wno-ignored-attributes
)

# ============================================================================
# Compiler Definitions
# ============================================================================

target_compile_definitions(musl PRIVATE
    # POSIX feature level
    -D_XOPEN_SOURCE=700
)

target_compile_definitions(musl PUBLIC
    # Mark this as musl
    -D__MUSL__=1
)

# ============================================================================
# Build CRT Objects (Startup/Shutdown code)
# ============================================================================

# These are separate object files linked at specific positions:
# Link order: crti.o -> [crtbegin.o] -> program objects -> [crtend.o] -> crtn.o

# Build Scrt1.o (static position-independent startup)
add_library(musl-Scrt1 OBJECT "${MUSL_SOURCE_DIR}/crt/Scrt1.c")
target_include_directories(musl-Scrt1 PRIVATE
    # MUST include src/include FIRST for features.h with 'hidden' and 'weak' macros
    "${MUSL_SOURCE_DIR}/src/include"
    "${MUSL_SOURCE_DIR}/src/internal"
    "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}"
    "${MUSL_GENERATED_DIR}"
)
target_compile_options(musl-Scrt1 PRIVATE
    -std=c99
    -ffreestanding
    -fno-stack-protector
    -fPIC
    -fno-builtin
)
add_dependencies(musl-Scrt1 musl-headers)

# Build crt1.o (static non-PIC startup) 
add_library(musl-crt1 OBJECT "${MUSL_SOURCE_DIR}/crt/crt1.c")
target_include_directories(musl-crt1 PRIVATE
    # MUST include src/include FIRST for features.h with 'hidden' and 'weak' macros
    "${MUSL_SOURCE_DIR}/src/include"
    "${MUSL_SOURCE_DIR}/src/internal"
    "${MUSL_SOURCE_DIR}/arch/${MUSL_ARCH}"
    "${MUSL_GENERATED_DIR}"
)
target_compile_options(musl-crt1 PRIVATE
    -std=c99
    -ffreestanding
    -fno-stack-protector
    -fno-builtin
)
add_dependencies(musl-crt1 musl-headers)

# Build crti.o (initialization prologue)
add_library(musl-crti OBJECT "${MUSL_SOURCE_DIR}/crt/${MUSL_ARCH}/crti.s")
set_source_files_properties(
    "${MUSL_SOURCE_DIR}/crt/${MUSL_ARCH}/crti.s"
    PROPERTIES LANGUAGE ASM
)
# -Qunused-arguments prevents errors about C flags being unused by assembler
target_compile_options(musl-crti PRIVATE -Qunused-arguments -Wa,--noexecstack)

# Build crtn.o (initialization epilogue)
add_library(musl-crtn OBJECT "${MUSL_SOURCE_DIR}/crt/${MUSL_ARCH}/crtn.s")
set_source_files_properties(
    "${MUSL_SOURCE_DIR}/crt/${MUSL_ARCH}/crtn.s"
    PROPERTIES LANGUAGE ASM
)
# -Qunused-arguments prevents errors about C flags being unused by assembler
target_compile_options(musl-crtn PRIVATE -Qunused-arguments -Wa,--noexecstack)

# ============================================================================
# Installation/Export (optional, for later)
# ============================================================================

# For now, musl library will be linked via target_link_libraries
# CRT objects will be linked via $<TARGET_OBJECTS:musl-crt*>

# ============================================================================
# Summary
# ============================================================================

message(STATUS "")
message(STATUS "=== Musl libc configuration ===")
message(STATUS "  Version:        ${MUSL_VERSION}")
message(STATUS "  Architecture:   ${MUSL_ARCH}")
message(STATUS "  Source files:   ${MUSL_SOURCE_COUNT}")
message(STATUS "  Math functions: EXCLUDED (using LLVM-libc)")
message(STATUS "  Unwinding:      ENABLED (-funwind-tables)")
message(STATUS "  Generated:      ${MUSL_GENERATED_DIR}/bits/")
message(STATUS "================================")
message(STATUS "")
