include(CheckCXXCompilerFlag)

set(LIBCXX_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libcxx")

set(SRCS
"${LIBCXX_SOURCE_DIR}/src/algorithm.cpp"
"${LIBCXX_SOURCE_DIR}/src/any.cpp"
"${LIBCXX_SOURCE_DIR}/src/atomic.cpp"
"${LIBCXX_SOURCE_DIR}/src/barrier.cpp"
"${LIBCXX_SOURCE_DIR}/src/bind.cpp"
"${LIBCXX_SOURCE_DIR}/src/charconv.cpp"
"${LIBCXX_SOURCE_DIR}/src/chrono.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable.cpp"
"${LIBCXX_SOURCE_DIR}/src/condition_variable_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/debug.cpp"
"${LIBCXX_SOURCE_DIR}/src/exception.cpp"
"${LIBCXX_SOURCE_DIR}/src/experimental/memory_resource.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/directory_iterator.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/int128_builtins.cpp"
"${LIBCXX_SOURCE_DIR}/src/filesystem/operations.cpp"
"${LIBCXX_SOURCE_DIR}/src/format.cpp"
"${LIBCXX_SOURCE_DIR}/src/functional.cpp"
"${LIBCXX_SOURCE_DIR}/src/future.cpp"
"${LIBCXX_SOURCE_DIR}/src/hash.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.cpp"
"${LIBCXX_SOURCE_DIR}/src/ios.instantiations.cpp"
"${LIBCXX_SOURCE_DIR}/src/iostream.cpp"
"${LIBCXX_SOURCE_DIR}/src/legacy_pointer_safety.cpp"
"${LIBCXX_SOURCE_DIR}/src/locale.cpp"
"${LIBCXX_SOURCE_DIR}/src/memory.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex.cpp"
"${LIBCXX_SOURCE_DIR}/src/mutex_destructor.cpp"
"${LIBCXX_SOURCE_DIR}/src/new.cpp"
"${LIBCXX_SOURCE_DIR}/src/optional.cpp"
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
"${LIBCXX_SOURCE_DIR}/src/utility.cpp"
"${LIBCXX_SOURCE_DIR}/src/valarray.cpp"
"${LIBCXX_SOURCE_DIR}/src/variant.cpp"
"${LIBCXX_SOURCE_DIR}/src/vector.cpp"
)

add_library(cxx ${SRCS})
set_target_properties(cxx PROPERTIES FOLDER "contrib/libcxx-cmake")

target_include_directories(cxx SYSTEM BEFORE PUBLIC
        $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${LIBCXX_SOURCE_DIR}>/src)
target_compile_definitions(cxx PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DLIBCXX_BUILDING_LIBCXXABI)

# Enable capturing stack traces for all exceptions.
if (USE_UNWIND)
    target_compile_definitions(cxx PUBLIC -DSTD_EXCEPTION_HAS_STACK_TRACE=1)
endif ()

if (USE_MUSL)
    target_compile_definitions(cxx PUBLIC -D_LIBCPP_HAS_MUSL_LIBC=1)
endif ()

# Override the deduced attribute support that causes error.
if (OS_DARWIN AND COMPILER_GCC)
    add_compile_definitions(_LIBCPP_INIT_PRIORITY_MAX)
endif ()

target_compile_options(cxx PUBLIC $<$<COMPILE_LANGUAGE:CXX>:-nostdinc++>)

# Third party library may have substandard code.
target_compile_options(cxx PRIVATE -w)

target_link_libraries(cxx PUBLIC cxxabi)

# For __udivmodti4, __divmodti4.
if (OS_DARWIN AND COMPILER_GCC)
    target_link_libraries(cxx PRIVATE gcc)
endif ()

install(
    TARGETS cxx
    EXPORT global
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib
    LIBRARY DESTINATION lib
)
