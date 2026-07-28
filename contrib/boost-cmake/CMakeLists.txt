set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/boost")

# filesystem

set (SRCS_FILESYSTEM
    "${LIBRARY_DIR}/libs/filesystem/src/codecvt_error_category.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/directory.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/exception.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/operations.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/path.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/path_traits.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/portability.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/unique_path.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/utf8_codecvt_facet.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/windows_file_codecvt.cpp"
)

add_library (_boost_filesystem ${SRCS_FILESYSTEM})
add_library (boost::filesystem ALIAS _boost_filesystem)
target_include_directories (_boost_filesystem SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

if (OS_LINUX)
    target_compile_definitions (_boost_filesystem PRIVATE
        BOOST_FILESYSTEM_HAS_POSIX_AT_APIS=1
    )
endif ()


# headers-only
add_library (_boost_headers_only INTERFACE)
add_library (boost::headers_only ALIAS _boost_headers_only)
target_include_directories (_boost_headers_only SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})

target_compile_definitions (_boost_headers_only INTERFACE
  BOOST_ASIO_STANDALONE=1
  BOOST_ASIO_HAS_STD_INVOKE_RESULT=1 # Avoid using of deprecated in c++ > 17 std::result_of
  BOOST_TIMER_ENABLE_DEPRECATED=1 # wordnet-blast (enabled via USE_NLP) uses Boost legacy timer classes
)

# iostreams

set (SRCS_IOSTREAMS
    "${LIBRARY_DIR}/libs/iostreams/src/file_descriptor.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/gzip.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/mapped_file.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/zlib.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/zstd.cpp"
)

add_library (_boost_iostreams ${SRCS_IOSTREAMS})
add_library (boost::iostreams ALIAS _boost_iostreams)
target_include_directories (_boost_iostreams PRIVATE ${LIBRARY_DIR})
target_link_libraries (_boost_iostreams PRIVATE ch_contrib::zlib)
target_link_libraries (_boost_iostreams PRIVATE ch_contrib::zstd)

# program_options

set (SRCS_PROGRAM_OPTIONS
    "${LIBRARY_DIR}/libs/program_options/src/cmdline.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/config_file.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/convert.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/options_description.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/parsers.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/positional_options.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/split.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/utf8_codecvt_facet.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/value_semantic.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/variables_map.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/winmain.cpp"
)

# Always compile this file with the highest possible level of optimizations, even in Debug builds.
# Otherwise the client takes too long to start and SQL stateless tests (many options to parse)
# https://github.com/ClickHouse/ClickHouse/issues/65745
set_source_files_properties(${SRCS_PROGRAM_OPTIONS} PROPERTIES COMPILE_FLAGS "-O3")

add_library (_boost_program_options ${SRCS_PROGRAM_OPTIONS})
add_library (boost::program_options ALIAS _boost_program_options)
target_include_directories (_boost_program_options SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

# regex

set (SRCS_REGEX
    "${LIBRARY_DIR}/libs/regex/src/posix_api.cpp"
    "${LIBRARY_DIR}/libs/regex/src/regex_debug.cpp"
    "${LIBRARY_DIR}/libs/regex/src/regex.cpp"
    "${LIBRARY_DIR}/libs/regex/src/static_mutex.cpp"
    "${LIBRARY_DIR}/libs/regex/src/wide_posix_api.cpp"
)

add_library (_boost_regex ${SRCS_REGEX})
add_library (boost::regex ALIAS _boost_regex)
target_include_directories (_boost_regex PRIVATE ${LIBRARY_DIR})

# system

set (SRCS_SYSTEM
    "${LIBRARY_DIR}/libs/system/src/error_code.cpp"
)

add_library (_boost_system ${SRCS_SYSTEM})
add_library (boost::system ALIAS _boost_system)
target_include_directories (_boost_system PRIVATE ${LIBRARY_DIR})

# context
option (BOOST_USE_UCONTEXT "Use ucontext_t for context switching of boost::fiber within boost::context" OFF)

enable_language(ASM)
SET(ASM_OPTIONS "-x assembler-with-cpp")

set (SRCS_CONTEXT
    "${LIBRARY_DIR}/libs/context/src/dummy.cpp"
    "${LIBRARY_DIR}/libs/context/src/posix/stack_traits.cpp"
)

if (ARCH_AARCH64)
    if (OS_DARWIN)
        set (SRCS_CONTEXT ${SRCS_CONTEXT}
            "${LIBRARY_DIR}/libs/context/src/asm/jump_arm64_aapcs_macho_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/make_arm64_aapcs_macho_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/ontop_arm64_aapcs_macho_gas.S"
        )
    else()
        set (SRCS_CONTEXT ${SRCS_CONTEXT}
            "${LIBRARY_DIR}/libs/context/src/asm/jump_arm64_aapcs_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/make_arm64_aapcs_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/ontop_arm64_aapcs_elf_gas.S"
        )
    endif()
elseif (ARCH_PPC64LE)
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_ppc64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_ppc64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_ppc64_sysv_elf_gas.S"
    )
elseif (ARCH_S390X)
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_s390x_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_s390x_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_s390x_sysv_elf_gas.S"
    )
elseif (ARCH_RISCV64)
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_riscv64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_riscv64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_riscv64_sysv_elf_gas.S"
    )
elseif (ARCH_LOONGARCH64)
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_loongarch64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_loongarch64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_loongarch64_sysv_elf_gas.S"
    )
elseif(OS_DARWIN)
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_x86_64_sysv_macho_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_x86_64_sysv_macho_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_x86_64_sysv_macho_gas.S"
    )
else()
    set (SRCS_CONTEXT ${SRCS_CONTEXT}
        "${LIBRARY_DIR}/libs/context/src/asm/jump_x86_64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/make_x86_64_sysv_elf_gas.S"
        "${LIBRARY_DIR}/libs/context/src/asm/ontop_x86_64_sysv_elf_gas.S"
    )
endif()

if (SANITIZE OR BOOST_USE_UCONTEXT)
    list (APPEND SRCS_CONTEXT
        "${LIBRARY_DIR}/libs/context/src/fiber.cpp"
        "${LIBRARY_DIR}/libs/context/src/continuation.cpp"
    )
endif()

add_library (_boost_context ${SRCS_CONTEXT})
add_library (boost::context ALIAS _boost_context)
target_include_directories (_boost_context PRIVATE ${LIBRARY_DIR})

if (SANITIZE OR BOOST_USE_UCONTEXT)
    target_compile_definitions(_boost_context PUBLIC BOOST_USE_UCONTEXT)
endif()

if (SANITIZE STREQUAL "address")
    target_compile_definitions(_boost_context PUBLIC BOOST_USE_ASAN)
elseif (SANITIZE STREQUAL "thread")
    target_compile_definitions(_boost_context PUBLIC BOOST_USE_TSAN)
endif()

# coroutine

set (SRCS_COROUTINE
  "${LIBRARY_DIR}/libs/coroutine/src/detail/coroutine_context.cpp"
  "${LIBRARY_DIR}/libs/coroutine/src/exceptions.cpp"
  "${LIBRARY_DIR}/libs/coroutine/src/posix/stack_traits.cpp"
)
add_library (_boost_coroutine ${SRCS_COROUTINE})
add_library (boost::coroutine ALIAS _boost_coroutine)
target_include_directories (_boost_coroutine PRIVATE ${LIBRARY_DIR})
target_link_libraries(_boost_coroutine PRIVATE _boost_context)

# graph

set (SRCS_GRAPH
    "${LIBRARY_DIR}/libs/graph/src/graphml.cpp"
    "${LIBRARY_DIR}/libs/graph/src/read_graphviz_new.cpp"
)

add_library (_boost_graph ${SRCS_GRAPH})
add_library (boost::graph ALIAS _boost_graph)
target_include_directories (_boost_graph PRIVATE ${LIBRARY_DIR})
target_link_libraries(_boost_graph PRIVATE _boost_regex)

# circular buffer
add_library(_boost_circular_buffer INTERFACE)
add_library(boost::circular_buffer ALIAS _boost_circular_buffer)
target_include_directories(_boost_circular_buffer SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})

# heap
add_library(_boost_heap INTERFACE)
add_library(boost::heap ALIAS _boost_heap)
target_include_directories(_boost_heap SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})
