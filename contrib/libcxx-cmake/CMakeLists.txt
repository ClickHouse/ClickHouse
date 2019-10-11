set(LIBCXX_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcxx)

set(SRCS
${LIBCXX_SOURCE_DIR}/src/optional.cpp
${LIBCXX_SOURCE_DIR}/src/variant.cpp
${LIBCXX_SOURCE_DIR}/src/chrono.cpp
${LIBCXX_SOURCE_DIR}/src/thread.cpp
${LIBCXX_SOURCE_DIR}/src/experimental/memory_resource.cpp
${LIBCXX_SOURCE_DIR}/src/iostream.cpp
${LIBCXX_SOURCE_DIR}/src/strstream.cpp
${LIBCXX_SOURCE_DIR}/src/ios.cpp
${LIBCXX_SOURCE_DIR}/src/future.cpp
${LIBCXX_SOURCE_DIR}/src/shared_mutex.cpp
${LIBCXX_SOURCE_DIR}/src/condition_variable.cpp
${LIBCXX_SOURCE_DIR}/src/hash.cpp
${LIBCXX_SOURCE_DIR}/src/string.cpp
${LIBCXX_SOURCE_DIR}/src/debug.cpp
${LIBCXX_SOURCE_DIR}/src/stdexcept.cpp
${LIBCXX_SOURCE_DIR}/src/utility.cpp
${LIBCXX_SOURCE_DIR}/src/any.cpp
${LIBCXX_SOURCE_DIR}/src/exception.cpp
${LIBCXX_SOURCE_DIR}/src/memory.cpp
${LIBCXX_SOURCE_DIR}/src/new.cpp
${LIBCXX_SOURCE_DIR}/src/valarray.cpp
${LIBCXX_SOURCE_DIR}/src/vector.cpp
${LIBCXX_SOURCE_DIR}/src/algorithm.cpp
${LIBCXX_SOURCE_DIR}/src/functional.cpp
${LIBCXX_SOURCE_DIR}/src/regex.cpp
${LIBCXX_SOURCE_DIR}/src/bind.cpp
${LIBCXX_SOURCE_DIR}/src/mutex.cpp
${LIBCXX_SOURCE_DIR}/src/charconv.cpp
${LIBCXX_SOURCE_DIR}/src/typeinfo.cpp
${LIBCXX_SOURCE_DIR}/src/locale.cpp
${LIBCXX_SOURCE_DIR}/src/filesystem/operations.cpp
${LIBCXX_SOURCE_DIR}/src/filesystem/int128_builtins.cpp
${LIBCXX_SOURCE_DIR}/src/filesystem/directory_iterator.cpp
${LIBCXX_SOURCE_DIR}/src/system_error.cpp
${LIBCXX_SOURCE_DIR}/src/random.cpp
)

add_library(cxx ${SRCS})

target_include_directories(cxx SYSTEM BEFORE PUBLIC $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>)
target_compile_definitions(cxx PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DLIBCXX_BUILDING_LIBCXXABI)

target_compile_options(cxx PUBLIC -nostdinc++ -Wno-reserved-id-macro)
if (OS_DARWIN AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
    target_compile_options(cxx PUBLIC -Wno-ctad-maybe-unsupported)
endif ()

target_link_libraries(cxx PUBLIC cxxabi)

install(
    TARGETS cxx
    EXPORT global
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib
    LIBRARY DESTINATION lib
)
