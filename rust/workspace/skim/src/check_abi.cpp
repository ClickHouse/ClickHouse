#include <exception>

/// ClickHouse patches std::exception with an embedded stack trace.
/// The CXX bridge code must be compiled with the same define,
/// otherwise sizeof(rust::Error) will be wrong and throw will
/// cause a heap-buffer-overflow.
static_assert(sizeof(std::exception) > sizeof(void *),
    "std::exception is too small - STD_EXCEPTION_HAS_STACK_TRACE=1 is missing from CXXFLAGS");
