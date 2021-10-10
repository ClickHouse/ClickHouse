find_program(SED_PROGRAM sed)
if(NOT SED_PROGRAM)
    message(FATAL_ERROR "You need the sed program to build ClickHouse with musl enabled.")
endif()

enable_language(ASM)

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/musl")

set (SRC_DIRS
    src/aio
    src/complex
    src/conf
    src/crypt
    src/ctype
    src/dirent
    src/env
    src/errno
    src/exit
    src/fcntl
    src/fenv
    src/include
    src/internal
    src/ipc
    src/ldso
    src/legacy
    src/linux
    src/locale
    src/malloc
    src/math
    src/misc
    src/mman
    src/mq
    src/multibyte
    src/network
    src/passwd
    src/prng
    src/process
    src/regex
    src/sched
    src/search
    src/select
    src/setjmp
    src/signal
    src/stat
    src/stdio
    src/stdlib
    src/string
    src/temp
    src/termios
    src/thread
    src/time
    src/unistd
)

if (ARCH_AMD64)
    set (ARCH x86_64)
elseif (ARCH_AARCH64)
    set (ARCH aarch64)
elseif (ARCH_PPC64LE)
    set (ARCH powerpc64)
else ()
    message(FATAL_ERROR "Unsupported architecture")
endif ()

set (SRCS)

foreach (PATH ${SRC_DIRS})
    file(GLOB NEW_FILES ${LIBRARY_DIR}/${PATH}/*.c ${LIBRARY_DIR}/${PATH}/${ARCH}/*.s ${LIBRARY_DIR}/${PATH}/${ARCH}/*.S)
    list(APPEND SRCS ${NEW_FILES})
endforeach ()

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/bits/alltypes.h"
    COMMAND mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/include/bits" && ${SED_PROGRAM} -f "${LIBRARY_DIR}/tools/mkalltypes.sed" "${LIBRARY_DIR}/arch/${ARCH}/bits/alltypes.h.in" "${LIBRARY_DIR}/include/alltypes.h.in" > "${CMAKE_CURRENT_BINARY_DIR}/include/bits/alltypes.h")

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/bits/syscall.h"
    COMMAND mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/include/bits" && ${SED_PROGRAM} -n -e s/__NR_/SYS_/p "${LIBRARY_DIR}/arch/${ARCH}/bits/syscall.h.in" > "${CMAKE_CURRENT_BINARY_DIR}/include/bits/syscall.h")

file (STRINGS ${LIBRARY_DIR}/VERSION VERSION)
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/src/internal/version.h"
    COMMAND mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/src/internal" && echo "#define VERSION \"${VERSION}\"" > "${CMAKE_CURRENT_BINARY_DIR}/src/internal/version.h"
    VERBATIM)

add_custom_target(
    musl_generated_headers
    DEPENDS
        "${CMAKE_CURRENT_BINARY_DIR}/include/bits/alltypes.h"
        "${CMAKE_CURRENT_BINARY_DIR}/include/bits/syscall.h"
        "${CMAKE_CURRENT_BINARY_DIR}/src/internal/version.h"
    COMMENT "Generating headers for Musl")

add_library(c ${SRCS})

add_dependencies(c musl_generated_headers)

target_include_directories(c SYSTEM PUBLIC
    ${LIBRARY_DIR}/arch/${ARCH}
    ${LIBRARY_DIR}/arch/generic
    ${LIBRARY_DIR}/src/include
    ${LIBRARY_DIR}/obj/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${LIBRARY_DIR}/include)

target_include_directories(c PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/src/internal
    ${LIBRARY_DIR}/src/internal)

target_compile_definitions(c PRIVATE _XOPEN_SOURCE=700)

add_library(crti OBJECT ${LIBRARY_DIR}/crt/${ARCH}/crti.s ${LIBRARY_DIR}/crt/Scrt1.c)
add_library(crtn OBJECT ${LIBRARY_DIR}/crt/${ARCH}/crtn.s)

target_compile_definitions(crti PRIVATE _XOPEN_SOURCE=700)
target_compile_definitions(crtn PRIVATE _XOPEN_SOURCE=700)

target_include_directories(crti PRIVATE
    ${LIBRARY_DIR}/arch/${ARCH}
    ${LIBRARY_DIR}/arch/generic
    ${LIBRARY_DIR}/src/include
    ${LIBRARY_DIR}/obj/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${LIBRARY_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/src/internal
    ${LIBRARY_DIR}/src/internal)

target_include_directories(crtn PRIVATE
    ${LIBRARY_DIR}/arch/${ARCH}
    ${LIBRARY_DIR}/arch/generic
    ${LIBRARY_DIR}/src/include
    ${LIBRARY_DIR}/obj/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${LIBRARY_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/src/internal
    ${LIBRARY_DIR}/src/internal)

add_library(crt $<TARGET_OBJECTS:crti> $<TARGET_OBJECTS:crtn>)

if (COMPILER_CLANG)
    target_compile_options(c PRIVATE -std=c99 -ffreestanding -Os -Wno-unused-command-line-argument)
    target_compile_options(crti PRIVATE -std=c99 -ffreestanding -Wno-unused-command-line-argument)
    target_compile_options(crtn PRIVATE -std=c99 -ffreestanding -Wno-unused-command-line-argument)
endif ()


add_custom_command(
    OUTPUT
        "${CMAKE_CURRENT_BINARY_DIR}/crt1.o"
        "${CMAKE_CURRENT_BINARY_DIR}/crti.o"
        "${CMAKE_CURRENT_BINARY_DIR}/crtbeginT.o"
        "${CMAKE_CURRENT_BINARY_DIR}/crtend.o"
        "${CMAKE_CURRENT_BINARY_DIR}/crtn.o"
    COMMAND
        mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/lib/x86_64-linux-gnu" && cp $<TARGET_OBJECTS:crti> $<TARGET_OBJECTS:crtn> "${CMAKE_CURRENT_BINARY_DIR}/lib/x86_64-linux-gnu"
    COMMAND_EXPAND_LISTS)

add_custom_target(copy_startfiles
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/crt1.o")

add_dependencies(c copy_startfiles)


add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/test.c"
    COMMAND echo "int main() { return 0; }" > "${CMAKE_CURRENT_BINARY_DIR}/test.c"
    VERBATIM)

add_custom_target(musl_test_source DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/test.c")

add_executable(musl_test "${CMAKE_CURRENT_BINARY_DIR}/test.c")

add_dependencies(musl_test musl_test_source)

target_link_libraries(musl_test c)

target_compile_options(musl_test PRIVATE --sysroot "${CMAKE_CURRENT_BINARY_DIR}")
