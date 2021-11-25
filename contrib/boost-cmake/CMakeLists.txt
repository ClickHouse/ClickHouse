option (USE_INTERNAL_BOOST_LIBRARY "Use internal Boost library" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_BOOST_LIBRARY)
    # 1.70 like in contrib/boost
    # 1.71 on CI
    set(BOOST_VERSION 1.71)

    find_package(Boost ${BOOST_VERSION} COMPONENTS
        system
        filesystem
        iostreams
        program_options
        regex
        context
        coroutine
        graph
    )

    if(Boost_INCLUDE_DIR AND Boost_FILESYSTEM_LIBRARY AND
        Boost_PROGRAM_OPTIONS_LIBRARY AND Boost_REGEX_LIBRARY AND Boost_SYSTEM_LIBRARY AND Boost_CONTEXT_LIBRARY AND
        Boost_COROUTINE_LIBRARY AND Boost_GRAPH_LIBRARY)

        set(EXTERNAL_BOOST_FOUND 1)

        add_library (_boost_headers_only INTERFACE)
        add_library (boost::headers_only ALIAS _boost_headers_only)
        target_include_directories (_boost_headers_only SYSTEM BEFORE INTERFACE ${Boost_INCLUDE_DIR})

        add_library (_boost_filesystem INTERFACE)
        add_library (_boost_iostreams INTERFACE)
        add_library (_boost_program_options INTERFACE)
        add_library (_boost_regex INTERFACE)
        add_library (_boost_system INTERFACE)
        add_library (_boost_context INTERFACE)
        add_library (_boost_coroutine INTERFACE)
        add_library (_boost_graph INTERFACE)

        target_link_libraries (_boost_filesystem INTERFACE ${Boost_FILESYSTEM_LIBRARY})
        target_link_libraries (_boost_iostreams INTERFACE ${Boost_IOSTREAMS_LIBRARY})
        target_link_libraries (_boost_program_options INTERFACE ${Boost_PROGRAM_OPTIONS_LIBRARY})
        target_link_libraries (_boost_regex INTERFACE ${Boost_REGEX_LIBRARY})
        target_link_libraries (_boost_system INTERFACE ${Boost_SYSTEM_LIBRARY})
        target_link_libraries (_boost_context INTERFACE ${Boost_CONTEXT_LIBRARY})
        target_link_libraries (_boost_coroutine INTERFACE ${Boost_COROUTINE_LIBRARY})
        target_link_libraries (_boost_graph INTERFACE ${Boost_GRAPH_LIBRARY})

        add_library (boost::filesystem ALIAS _boost_filesystem)
        add_library (boost::iostreams ALIAS _boost_iostreams)
        add_library (boost::program_options ALIAS _boost_program_options)
        add_library (boost::regex ALIAS _boost_regex)
        add_library (boost::system ALIAS _boost_system)
        add_library (boost::context ALIAS _boost_context)
        add_library (boost::coroutine ALIAS _boost_coroutine)
        add_library (boost::graph ALIAS _boost_graph)
    else()
        set(EXTERNAL_BOOST_FOUND 0)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system boost")
    endif()
endif()

if (NOT EXTERNAL_BOOST_FOUND)
    set (USE_INTERNAL_BOOST_LIBRARY 1)
    set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/boost")

    # filesystem

    set (SRCS_FILESYSTEM
        "${LIBRARY_DIR}/libs/filesystem/src/codecvt_error_category.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/operations.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/path_traits.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/path.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/portability.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/unique_path.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/utf8_codecvt_facet.cpp"
        "${LIBRARY_DIR}/libs/filesystem/src/windows_file_codecvt.cpp"
    )

    add_library (_boost_filesystem ${SRCS_FILESYSTEM})
    add_library (boost::filesystem ALIAS _boost_filesystem)
    target_include_directories (_boost_filesystem SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

    # headers-only

    add_library (_boost_headers_only INTERFACE)
    add_library (boost::headers_only ALIAS _boost_headers_only)
    target_include_directories (_boost_headers_only SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})

    # asio

    target_compile_definitions (_boost_headers_only INTERFACE BOOST_ASIO_STANDALONE=1)

    # iostreams

    set (SRCS_IOSTREAMS
        "${LIBRARY_DIR}/libs/iostreams/src/file_descriptor.cpp"
        "${LIBRARY_DIR}/libs/iostreams/src/gzip.cpp"
        "${LIBRARY_DIR}/libs/iostreams/src/mapped_file.cpp"
        "${LIBRARY_DIR}/libs/iostreams/src/zlib.cpp"
    )

    add_library (_boost_iostreams ${SRCS_IOSTREAMS})
    add_library (boost::iostreams ALIAS _boost_iostreams)
    target_include_directories (_boost_iostreams PRIVATE ${LIBRARY_DIR})
    target_link_libraries (_boost_iostreams PRIVATE ${ZLIB_LIBRARIES})

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

    add_library (_boost_program_options ${SRCS_PROGRAM_OPTIONS})
    add_library (boost::program_options ALIAS _boost_program_options)
    target_include_directories (_boost_program_options SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

    # regex

    set (SRCS_REGEX
        "${LIBRARY_DIR}/libs/regex/src/c_regex_traits.cpp"
        "${LIBRARY_DIR}/libs/regex/src/cpp_regex_traits.cpp"
        "${LIBRARY_DIR}/libs/regex/src/cregex.cpp"
        "${LIBRARY_DIR}/libs/regex/src/fileiter.cpp"
        "${LIBRARY_DIR}/libs/regex/src/icu.cpp"
        "${LIBRARY_DIR}/libs/regex/src/instances.cpp"
        "${LIBRARY_DIR}/libs/regex/src/internals.hpp"
        "${LIBRARY_DIR}/libs/regex/src/posix_api.cpp"
        "${LIBRARY_DIR}/libs/regex/src/regex_debug.cpp"
        "${LIBRARY_DIR}/libs/regex/src/regex_raw_buffer.cpp"
        "${LIBRARY_DIR}/libs/regex/src/regex_traits_defaults.cpp"
        "${LIBRARY_DIR}/libs/regex/src/regex.cpp"
        "${LIBRARY_DIR}/libs/regex/src/static_mutex.cpp"
        "${LIBRARY_DIR}/libs/regex/src/usinstances.cpp"
        "${LIBRARY_DIR}/libs/regex/src/w32_regex_traits.cpp"
        "${LIBRARY_DIR}/libs/regex/src/wc_regex_traits.cpp"
        "${LIBRARY_DIR}/libs/regex/src/wide_posix_api.cpp"
        "${LIBRARY_DIR}/libs/regex/src/winstances.cpp"
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
    enable_language(ASM)
    SET(ASM_OPTIONS "-x assembler-with-cpp")

    set (SRCS_CONTEXT
        "${LIBRARY_DIR}/libs/context/src/dummy.cpp"
        "${LIBRARY_DIR}/libs/context/src/execution_context.cpp"
        "${LIBRARY_DIR}/libs/context/src/posix/stack_traits.cpp"
    )

    if (SANITIZE AND (SANITIZE STREQUAL "address" OR SANITIZE STREQUAL "thread"))
        add_compile_definitions(BOOST_USE_UCONTEXT)

        if (SANITIZE STREQUAL "address")
            add_compile_definitions(BOOST_USE_ASAN)
        elseif (SANITIZE STREQUAL "thread")
            add_compile_definitions(BOOST_USE_TSAN)
        endif()

        set (SRCS_CONTEXT ${SRCS_CONTEXT}
                "${LIBRARY_DIR}/libs/context/src/fiber.cpp"
                "${LIBRARY_DIR}/libs/context/src/continuation.cpp"
        )
    endif()
    if (ARCH_ARM)
        set (SRCS_CONTEXT ${SRCS_CONTEXT}
            "${LIBRARY_DIR}/libs/context/src/asm/jump_arm64_aapcs_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/make_arm64_aapcs_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/ontop_arm64_aapcs_elf_gas.S"
        )
    elseif (ARCH_PPC64LE)
        set (SRCS_CONTEXT ${SRCS_CONTEXT}
            "${LIBRARY_DIR}/libs/context/src/asm/jump_ppc64_sysv_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/make_ppc64_sysv_elf_gas.S"
            "${LIBRARY_DIR}/libs/context/src/asm/ontop_ppc64_sysv_elf_gas.S"
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

    add_library (_boost_context ${SRCS_CONTEXT})
    add_library (boost::context ALIAS _boost_context)
    target_include_directories (_boost_context PRIVATE ${LIBRARY_DIR})

    # coroutine

    set (SRCS_COROUTINE
      "${LIBRARY_DIR}/libs/coroutine/detail/coroutine_context.cpp"
      "${LIBRARY_DIR}/libs/coroutine/exceptions.cpp"
      "${LIBRARY_DIR}/libs/coroutine/posix/stack_traits.cpp"
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

endif ()
