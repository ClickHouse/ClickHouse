# Set standard, system and compiler libraries explicitly.
# This is intended for more control of what we are linking.

set (DEFAULT_LIBS "-nodefaultlibs")

if (OS_LINUX)
    # We need builtins from Clang's RT even without libcxx - for ubsan+int128.
    # See https://bugs.llvm.org/show_bug.cgi?id=16404
    if (COMPILER_CLANG)
        execute_process (COMMAND ${CMAKE_CXX_COMPILER} --print-file-name=libclang_rt.builtins-${CMAKE_SYSTEM_PROCESSOR}.a OUTPUT_VARIABLE BUILTINS_LIBRARY OUTPUT_STRIP_TRAILING_WHITESPACE)
    else ()
        set (BUILTINS_LIBRARY "-lgcc")
    endif ()

    set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt")

    message(STATUS "Default libraries: ${DEFAULT_LIBS}")
endif ()

# NOTE: this probably has no effect.
set (CMAKE_CXX_IMPLICIT_LINK_LIBRARIES "")
set (CMAKE_C_IMPLICIT_LINK_LIBRARIES "")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})
