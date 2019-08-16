set(LIBUNWIND_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libunwind)

set(LIBUNWIND_CXX_SOURCES
    ${LIBUNWIND_SOURCE_DIR}/src/libunwind.cpp
    ${LIBUNWIND_SOURCE_DIR}/src/Unwind-EHABI.cpp
    ${LIBUNWIND_SOURCE_DIR}/src/Unwind-seh.cpp)
if (APPLE)
    set(LIBUNWIND_CXX_SOURCES ${LIBUNWIND_CXX_SOURCES} ${LIBUNWIND_SOURCE_DIR}/src/Unwind_AppleExtras.cpp)
endif ()

set(LIBUNWIND_C_SOURCES
    ${LIBUNWIND_SOURCE_DIR}/src/UnwindLevel1.c
    ${LIBUNWIND_SOURCE_DIR}/src/UnwindLevel1-gcc-ext.c
    ${LIBUNWIND_SOURCE_DIR}/src/Unwind-sjlj.c)
set_source_files_properties(${LIBUNWIND_C_SOURCES} PROPERTIES COMPILE_FLAGS "-std=c99")

set(LIBUNWIND_ASM_SOURCES
    ${LIBUNWIND_SOURCE_DIR}/src/UnwindRegistersRestore.S
    ${LIBUNWIND_SOURCE_DIR}/src/UnwindRegistersSave.S)
set_source_files_properties(${LIBUNWIND_ASM_SOURCES} PROPERTIES LANGUAGE C)

set(LIBUNWIND_SOURCES
    ${LIBUNWIND_CXX_SOURCES}
    ${LIBUNWIND_C_SOURCES}
    ${LIBUNWIND_ASM_SOURCES})

add_library(unwind_static ${LIBUNWIND_SOURCES})

target_include_directories(unwind_static SYSTEM BEFORE PUBLIC ${LIBUNWIND_SOURCE_DIR}/include)
target_compile_definitions(unwind_static PRIVATE -D_LIBUNWIND_NO_HEAP=1 -D_DEBUG -D_LIBUNWIND_IS_NATIVE_ONLY)
target_compile_options(unwind_static PRIVATE -fno-exceptions -funwind-tables -fno-sanitize=all -nostdinc++ -fno-rtti)
target_link_libraries(unwind_static PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
