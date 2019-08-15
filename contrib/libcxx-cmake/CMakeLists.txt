set(LIBCXX_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/libcxx)
#set(LIBCXX_BINARY_DIR ${ClickHouse_BINARY_DIR}/contrib/libcxx)

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
#${LIBCXX_SOURCE_DIR}/src/support/win32/support.cpp
#${LIBCXX_SOURCE_DIR}/src/support/win32/locale_win32.cpp
#${LIBCXX_SOURCE_DIR}/src/support/win32/thread_win32.cpp
#${LIBCXX_SOURCE_DIR}/src/support/solaris/xlocale.cpp
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

add_library(cxx_static ${SRCS})

target_include_directories(cxx_static PUBLIC ${LIBCXX_SOURCE_DIR}/include)
target_compile_definitions(cxx_static PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DLIBCXX_BUILDING_LIBCXXABI)
target_compile_options(cxx_static PRIVATE -nostdinc++)

