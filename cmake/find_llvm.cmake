option (ENABLE_EMBEDDED_COMPILER "Set to TRUE to enable support for 'compile' option for query execution" 1)

if (ENABLE_EMBEDDED_COMPILER)
    set (LLVM_PATHS "/usr/local/lib/llvm")

    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        find_package(LLVM CONFIG PATHS ${LLVM_PATHS})
    else ()
        find_package(LLVM 5 CONFIG PATHS ${LLVM_PATHS})
    endif ()

    if (LLVM_FOUND)
        # Remove dynamically-linked zlib and libedit from LLVM's dependencies:
        set_target_properties(LLVMSupport PROPERTIES INTERFACE_LINK_LIBRARIES "-lpthread;LLVMDemangle")
        set_target_properties(LLVMLineEditor PROPERTIES INTERFACE_LINK_LIBRARIES "LLVMSupport")

        message(STATUS "LLVM version: ${LLVM_PACKAGE_VERSION}")
        message(STATUS "LLVM Include Directory: ${LLVM_INCLUDE_DIRS}")
        message(STATUS "LLVM Library Directory: ${LLVM_LIBRARY_DIRS}")
        message(STATUS "LLVM C++ Compiler: ${LLVM_CXXFLAGS}")

        option(LLVM_HAS_RTTI "Enable if LLVM was build with RTTI enabled" ON)

        set (USE_EMBEDDED_COMPILER 1)
    endif()
endif()
