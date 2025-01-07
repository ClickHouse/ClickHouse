set (DEFAULT_LIBS "-nodefaultlibs")

if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "amd64")
    set(system_processor "x86_64")
else ()
    set(system_processor "${CMAKE_SYSTEM_PROCESSOR}")
endif ()

file(GLOB bprefix "/usr/local/llvm${COMPILER_VERSION_MAJOR}/lib/clang/${COMPILER_VERSION_MAJOR}/lib/${system_processor}-portbld-freebsd*/")
message(STATUS "-Bprefix: ${bprefix}")

execute_process(COMMAND
    ${CMAKE_CXX_COMPILER} -Bprefix=${bprefix} --print-file-name=libclang_rt.builtins-${system_processor}.a
    OUTPUT_VARIABLE BUILTINS_LIBRARY
    COMMAND_ERROR_IS_FATAL ANY
    OUTPUT_STRIP_TRAILING_WHITESPACE)
# --print-file-name simply prints what you passed in case of nothing was resolved, so let's try one other possible option
if (BUILTINS_LIBRARY STREQUAL "libclang_rt.builtins-${system_processor}.a")
    execute_process(COMMAND
        ${CMAKE_CXX_COMPILER} -Bprefix=${bprefix} --print-file-name=libclang_rt.builtins.a
        OUTPUT_VARIABLE BUILTINS_LIBRARY
        COMMAND_ERROR_IS_FATAL ANY
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
if (BUILTINS_LIBRARY STREQUAL "libclang_rt.builtins.a")
    message(FATAL_ERROR "libclang_rt.builtins had not been found")
endif()

set (DEFAULT_LIBS "${DEFAULT_LIBS} ${BUILTINS_LIBRARY} ${COVERAGE_OPTION} -lc -lm -lrt -lpthread")

message(STATUS "Default libraries: ${DEFAULT_LIBS}")

set(CMAKE_CXX_STANDARD_LIBRARIES ${DEFAULT_LIBS})
set(CMAKE_C_STANDARD_LIBRARIES ${DEFAULT_LIBS})

# Unfortunately '-pthread' doesn't work with '-nodefaultlibs'.
# Just make sure we have pthreads at all.
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

include (cmake/unwind.cmake)
include (cmake/cxx.cmake)
link_libraries(global-group)

target_link_libraries(global-group INTERFACE
    $<TARGET_PROPERTY:global-libs,INTERFACE_LINK_LIBRARIES>
)
