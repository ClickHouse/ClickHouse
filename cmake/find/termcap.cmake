if (ENABLE_EMBEDDED_COMPILER AND NOT USE_INTERNAL_LLVM_LIBRARY AND USE_STATIC_LIBRARIES)
    find_library (TERMCAP_LIBRARY tinfo)
    if (NOT TERMCAP_LIBRARY)
        find_library (TERMCAP_LIBRARY ncurses)
    endif()
    if (NOT TERMCAP_LIBRARY)
        find_library (TERMCAP_LIBRARY termcap)
    endif()

    if (NOT TERMCAP_LIBRARY)
        message (FATAL_ERROR "Statically Linking external LLVM requires termcap")
    endif()

    target_link_libraries(LLVMSupport INTERFACE ${TERMCAP_LIBRARY})

    message (STATUS "Using termcap: ${TERMCAP_LIBRARY}")
endif()
