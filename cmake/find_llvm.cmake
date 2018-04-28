option (ENABLE_EMBEDDED_COMPILER "Set to TRUE to enable support for 'compile' option for query execution" 1)

if (ENABLE_EMBEDDED_COMPILER)
    # Based on source code of YT.
    # Authors: Ivan Puzyrevskiy, Alexey Lukyanchikov, Ruslan Savchenko.

    #  Find LLVM includes and libraries.
    #
    #  LLVM_VERSION      - LLVM version.
    #  LLVM_INCLUDE_DIRS - Directory containing LLVM headers.
    #  LLVM_LIBRARY_DIRS - Directory containing LLVM libraries.
    #  LLVM_CXXFLAGS     - C++ compiler flags for files that include LLVM headers.
    #  LLVM_FOUND        - True if LLVM was found.

    #  llvm_map_components_to_libraries - Maps LLVM used components to required libraries.
    #  Usage: llvm_map_components_to_libraries(REQUIRED_LLVM_LIBRARIES core jit interpreter native ...)

    if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(LLVM_VERSION_POSTFIX "${COMPILER_POSTFIX}" CACHE STRING "")
    else()
        if (ARCH_FREEBSD)
           set(LLVM_VERSION_POSTFIX "50" CACHE STRING "")
        else()
           set(LLVM_VERSION_POSTFIX "-5.0" CACHE STRING "")
        endif()
    endif()

    find_program(LLVM_CONFIG_EXECUTABLE
        NAMES llvm-config${LLVM_VERSION_POSTFIX} llvm-config llvm-config-devel
        PATHS $ENV{LLVM_ROOT}/bin)

    mark_as_advanced(LLVM_CONFIG_EXECUTABLE)

    if(NOT LLVM_CONFIG_EXECUTABLE)
        message(WARNING "Cannot find LLVM (looking for `llvm-config${LLVM_VERSION_POSTFIX}`, `llvm-config`, `llvm-config-devel`). Please, provide LLVM_ROOT environment variable.")
    else()
        set(LLVM_FOUND TRUE)

        execute_process(
            COMMAND ${LLVM_CONFIG_EXECUTABLE} --version
            OUTPUT_VARIABLE LLVM_VERSION
            OUTPUT_STRIP_TRAILING_WHITESPACE)

        if(LLVM_VERSION VERSION_LESS "5")
            message(FATAL_ERROR "LLVM 5+ is required. You have ${LLVM_VERSION} (${LLVM_CONFIG_EXECUTABLE})")
        endif()

        message(STATUS "LLVM config: ${LLVM_CONFIG_EXECUTABLE}; version: ${LLVM_VERSION}")

        execute_process(
            COMMAND ${LLVM_CONFIG_EXECUTABLE} --includedir
            OUTPUT_VARIABLE LLVM_INCLUDE_DIRS
            OUTPUT_STRIP_TRAILING_WHITESPACE)

        execute_process(
            COMMAND ${LLVM_CONFIG_EXECUTABLE} --libdir
            OUTPUT_VARIABLE LLVM_LIBRARY_DIRS
            OUTPUT_STRIP_TRAILING_WHITESPACE)

        execute_process(
            COMMAND ${LLVM_CONFIG_EXECUTABLE} --cxxflags
            OUTPUT_VARIABLE LLVM_CXXFLAGS
            OUTPUT_STRIP_TRAILING_WHITESPACE)

        execute_process(
            COMMAND ${LLVM_CONFIG_EXECUTABLE} --targets-built
            OUTPUT_VARIABLE LLVM_TARGETS_BUILT
            OUTPUT_STRIP_TRAILING_WHITESPACE)

        string(REPLACE " " ";" LLVM_TARGETS_BUILT "${LLVM_TARGETS_BUILT}")

        if (USE_STATIC_LIBRARIES)
            set (LLVM_CONFIG_ADD "--link-static")
        endif()

        # Get the link libs we need.
        function(llvm_map_components_to_libraries RESULT)
            execute_process(
                COMMAND ${LLVM_CONFIG_EXECUTABLE} ${LLVM_CONFIG_ADD} --libs ${ARGN}
                OUTPUT_VARIABLE _tmp
                OUTPUT_STRIP_TRAILING_WHITESPACE)

            string(REPLACE " " ";" _libs_module "${_tmp}")

            #message(STATUS "LLVM Libraries for '${ARGN}': ${_libs_module}")

            execute_process(
                COMMAND ${LLVM_CONFIG_EXECUTABLE} --system-libs ${ARGN}
                OUTPUT_VARIABLE _libs_system
                OUTPUT_STRIP_TRAILING_WHITESPACE)

            string(REPLACE "\n" " " _libs_system "${_libs_system}")
            string(REPLACE "    " " " _libs_system "${_libs_system}")
            string(REPLACE " "    ";" _libs_system "${_libs_system}")

            set(${RESULT} ${_libs_module} ${_libs_system} PARENT_SCOPE)
        endfunction(llvm_map_components_to_libraries)

        message(STATUS "LLVM Include Directory: ${LLVM_INCLUDE_DIRS}")
        message(STATUS "LLVM Library Directory: ${LLVM_LIBRARY_DIRS}")
        message(STATUS "LLVM C++ Compiler: ${LLVM_CXXFLAGS}")
    endif()

    if (LLVM_FOUND AND LLVM_INCLUDE_DIRS AND LLVM_LIBRARY_DIRS)
        set (USE_EMBEDDED_COMPILER 1)
    endif()
endif()
