# Source: contrib/llvm-project/compiler-rt/lib/interception/CMakeLists.txt
#   - INTERCEPTION_SOURCES (the full list there)
#
# All five files compile on every platform — each wraps its body in a
# platform #ifdef, so e.g. interception_win.cpp produces an empty object
# on Linux.

set(INTERCEPTION_SOURCES
    interception_aix.cpp
    interception_linux.cpp
    interception_mac.cpp
    interception_win.cpp
    interception_type_test.cpp
)
