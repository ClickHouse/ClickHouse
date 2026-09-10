# UBSan is split into common runtime, standalone runtime, and C++ ABI parts.
#
# Source: contrib/llvm-project/compiler-rt/lib/ubsan/CMakeLists.txt
#   - UBSAN_SOURCES           — the RTUbsan core
#   - UBSAN_STANDALONE_SOURCES — RTUbsan_standalone
#   - UBSAN_CXX_SOURCES       — RTUbsan_cxx (only built when SANITIZER_CAN_USE_CXXABI;
#                                always true on Linux/FreeBSD with libcxxabi)

set(UBSAN_SOURCES
    ubsan_diag.cpp
    ubsan_init.cpp
    ubsan_flags.cpp
    ubsan_handlers.cpp
    ubsan_monitor.cpp
    ubsan_value.cpp
)

set(UBSAN_STANDALONE_SOURCES
    ubsan_diag_standalone.cpp
    ubsan_init_standalone.cpp
    ubsan_signals_standalone.cpp
)

# Sources that exercise C++ RTTI (compiled with RTTI enabled).
set(UBSAN_CXX_SOURCES
    ubsan_handlers_cxx.cpp
    ubsan_type_hash.cpp
    ubsan_type_hash_itanium.cpp
    ubsan_type_hash_win.cpp
)
