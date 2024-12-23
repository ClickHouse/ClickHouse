set(LIBUNWIND_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libunwind")

set(LIBUNWIND_CXX_SOURCES
    "${LIBUNWIND_SOURCE_DIR}/src/libunwind.cpp"
    "${LIBUNWIND_SOURCE_DIR}/src/Unwind-EHABI.cpp"
    "${LIBUNWIND_SOURCE_DIR}/src/Unwind-seh.cpp")

set(LIBUNWIND_C_SOURCES
    "${LIBUNWIND_SOURCE_DIR}/src/UnwindLevel1.c"
    "${LIBUNWIND_SOURCE_DIR}/src/UnwindLevel1-gcc-ext.c"
    "${LIBUNWIND_SOURCE_DIR}/src/Unwind-sjlj.c"
    # Use unw_backtrace to override libgcc's backtrace symbol for better ABI compatibility
    unwind-override.c)
set_source_files_properties(${LIBUNWIND_C_SOURCES} PROPERTIES COMPILE_FLAGS "-std=c99")

set(LIBUNWIND_ASM_SOURCES
    "${LIBUNWIND_SOURCE_DIR}/src/UnwindRegistersRestore.S"
    "${LIBUNWIND_SOURCE_DIR}/src/UnwindRegistersSave.S")

enable_language(ASM)

set(LIBUNWIND_SOURCES
    ${LIBUNWIND_CXX_SOURCES}
    ${LIBUNWIND_C_SOURCES}
    ${LIBUNWIND_ASM_SOURCES})

add_library(unwind ${LIBUNWIND_SOURCES})
set_target_properties(unwind PROPERTIES FOLDER "contrib/libunwind-cmake")

target_include_directories(unwind SYSTEM BEFORE PUBLIC $<BUILD_INTERFACE:${LIBUNWIND_SOURCE_DIR}/include>)
target_compile_definitions(unwind PRIVATE -D_LIBUNWIND_NO_HEAP=1)
target_compile_definitions(unwind PRIVATE -D_LIBUNWIND_REMEMBER_STACK_ALLOC=1)
# NOTE: from this macros sizeof(unw_context_t)/sizeof(unw_cursor_t) is depends, so it should be set always
target_compile_definitions(unwind PUBLIC -D_LIBUNWIND_IS_NATIVE_ONLY)

# We should enable optimizations (otherwise it will be too slow in debug)
# and disable sanitizers (otherwise infinite loop may happen)
target_compile_options(unwind PRIVATE -O3 -fno-exceptions -funwind-tables -fno-sanitize=all $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++ -fno-rtti>)

target_compile_options(unwind PRIVATE -Wno-unused-but-set-variable)

# The library is using register variables that are bound to specific registers
# Example: DwarfInstructions.hpp: register unsigned long long x16 __asm("x16") = cfa;
target_compile_options(unwind PRIVATE "$<$<COMPILE_LANGUAGE:CXX>:-Wno-register>")

install(
    TARGETS unwind
    EXPORT global
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
