set(LIBCXX_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxx")
set(LIBCXX_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/llvm-project/libcxx")
set(LIBCXX_GENERATED_INCLUDE_DIR "${LIBCXX_BINARY_DIR}/include/c++/v1")

set(SRCS
"${LIBCXX_SOURCE_DIR}/src/algorithm.cpp"
"${LIBCXX_SOURCE_DIR}/src/any.cpp"
"${LIBCXX_SOURCE_DIR}/src/atomic.cpp"
"${LIBCXX_SOURCE_DIR}/src/expected.cpp"
"${LIBCXX_SOURCE_DIR}/src/barrier.cpp"
"${LIBCXX_SOURCE_DIR}/src/bind.cpp"
"${LIBCXX_SOURCE_DIR}/src/call_once.cpp"
"${LIBCXX_SOURCE_DIR}/src/charconv.cpp"
"${LIBCXX_SOURCE_DIR}/src/chrono.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/error_category.cpp"
"${LIBCXX_SOURCE_DIR}/src/exception.cpp"
"${LIBCXX_SOURCE_DIR}/src/experimental/keep.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/directory_entry.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/directory_iterator.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/filesystem_clock.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/filesystem_error.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/int128_builtins.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/operations.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/path.cpp"
"${LIBCXX_SOURCE_DIR}/src/fstream.cpp"
"${LIBCXX_SOURCE_DIR}/src/functional.cpp"
"${LIBCXX_SOURCE_DIR}/src/future.cpp"
"${LIBCXX_SOURCE_DIR}/src/hash.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.instantiations.cpp"
"${LIBCXX_SOURCE_DIR}/src/iostream.cpp"
"${LIBCXX_SOURCE_DIR}/src/legacy_pointer_safety.cpp"
"${LIBCXX_SOURCE_DIR}/src/locale.cpp"
"${LIBCXX_SOURCE_DIR}/src/memory.cpp"
"${LIBCXX_SOURCE_DIR}/src/memory_resource.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/new.cpp"
"${LIBCXX_SOURCE_DIR}/src/new_handler.cpp"
"${LIBCXX_SOURCE_DIR}/src/new_helpers.cpp"
"${LIBCXX_SOURCE_DIR}/src/optional.cpp"
"${LIBCXX_SOURCE_DIR}/src/ostream.cpp"
"${LIBCXX_SOURCE_DIR}/src/print.cpp"
"${LIBCXX_SOURCE_DIR}/src/random.cpp"
"${LIBCXX_SOURCE_DIR}/src/random_shuffle.cpp"
"${LIBCXX_SOURCE_DIR}/src/regex.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/d2fixed.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/d2s.cpp"
"${LIBCXX_SOURCE_DIR}/src/ryu/f2s.cpp"
"${LIBCXX_SOURCE_DIR}/src/shared_mutex.cpp"
"${LIBCXX_SOURCE_DIR}/src/stdexcept.cpp"
"${LIBCXX_SOURCE_DIR}/src/string.cpp"
"${LIBCXX_SOURCE_DIR}/src/strstream.cpp"
"${LIBCXX_SOURCE_DIR}/src/system_error.cpp"
"${LIBCXX_SOURCE_DIR}/src/thread.cpp"
"${LIBCXX_SOURCE_DIR}/src/typeinfo.cpp"
"${LIBCXX_SOURCE_DIR}/src/valarray.cpp"
"${LIBCXX_SOURCE_DIR}/src/variant.cpp"
"${LIBCXX_SOURCE_DIR}/src/vector.cpp"
"${LIBCXX_SOURCE_DIR}/src/verbose_abort.cpp"
)

configure_file("${LIBCXX_SOURCE_DIR}/include/__assertion_handler" "${LIBCXX_GENERATED_INCLUDE_DIR}/__assertion_handler" COPYONLY)
configure_file("${LIBCXX_SOURCE_DIR}/include/__config_site" "${LIBCXX_GENERATED_INCLUDE_DIR}/__config_site" COPYONLY)

set(_all_includes "${LIBCXX_GENERATED_INCLUDE_DIR}/__config_site" "${LIBCXX_GENERATED_INCLUDE_DIR}/__assertion_handler")

add_custom_target(generate-cxx-headers ALL DEPENDS ${_all_includes})

add_library(cxx-headers INTERFACE)
add_dependencies(cxx-headers generate-cxx-headers)
target_include_directories(cxx-headers INTERFACE ${LIBCXX_GENERATED_INCLUDE_DIR})

add_library(cxx ${SRCS})
set_target_properties(cxx PROPERTIES FOLDER "contrib/libcxx-cmake")

target_include_directories(cxx SYSTEM BEFORE PRIVATE $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/src>)
target_include_directories(cxx SYSTEM BEFORE PUBLIC  $<$<COMPILE_LANGUAGE:CXX>:$<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>>)
target_compile_definitions(cxx PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DLIBCXX_BUILDING_LIBCXXABI)

# Enable capturing stack traces for all exceptions.
target_compile_definitions(cxx PUBLIC -DSTD_EXCEPTION_HAS_STACK_TRACE=1)

if (USE_MUSL)
    target_compile_definitions(cxx PUBLIC -D_LIBCPP_HAS_MUSL_LIBC=1)
endif ()

target_compile_options(cxx PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)

# Third party library may have substandard code.
target_compile_options(cxx PRIVATE -w)

# Enable support for Clang-Thread-Safety-Analysis in libcxx
target_compile_definitions(cxx PUBLIC -D_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS)

target_link_libraries(cxx PUBLIC cxx-headers)
target_link_libraries(cxx PUBLIC cxxabi)
