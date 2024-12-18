set(LIBCXXABI_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/libcxxabi")

set(SRCS
"${LIBCXXABI_SOURCE_DIR}/src/abort_message.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_aux_runtime.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_default_handlers.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_demangle.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_exception.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_exception_storage.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_guard.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_handlers.cpp"
# "${LIBCXXABI_SOURCE_DIR}/src/cxa_noexception.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_personality.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_thread_atexit.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_vector.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/cxa_virtual.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/fallback_malloc.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/private_typeinfo.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_exception.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_new_delete.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_stdexcept.cpp"
"${LIBCXXABI_SOURCE_DIR}/src/stdlib_typeinfo.cpp"
)

add_library(cxxabi ${SRCS})
set_target_properties(cxxabi PROPERTIES FOLDER "contrib/libcxxabi-cmake")

# Third party library may have substandard code.
target_compile_options(cxxabi PRIVATE -w)

target_include_directories(cxxabi SYSTEM BEFORE
    PUBLIC $<BUILD_INTERFACE:${LIBCXXABI_SOURCE_DIR}/include>
    PRIVATE $<BUILD_INTERFACE:${LIBCXXABI_SOURCE_DIR}/../libcxx/include>
    PRIVATE $<BUILD_INTERFACE:${LIBCXXABI_SOURCE_DIR}/../libcxx/src>
)
target_compile_definitions(cxxabi PRIVATE -D_LIBCPP_BUILDING_LIBRARY -DHAS_THREAD_LOCAL)
target_compile_options(cxxabi PRIVATE -nostdinc++ -fno-sanitize=undefined -Wno-macro-redefined) # If we don't disable UBSan, infinite recursion happens in dynamic_cast.
target_link_libraries(cxxabi PUBLIC unwind)

# Enable capturing stack traces for all exceptions.
target_compile_definitions(cxxabi PUBLIC -DSTD_EXCEPTION_HAS_STACK_TRACE=1)

install(
    TARGETS cxxabi
    EXPORT global
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib
    LIBRARY DESTINATION lib
)
