# Default libraries for the Emscripten/WebAssembly target.
#
# Unlike the other platforms, this one does not use `-nodefaultlibs` and does not build its own
# compiler-rt, libc++, libc++abi or libunwind: the Emscripten sysroot already provides all of
# them, along with a musl-derived libc and a pthread implementation on top of Web Workers.
# Overriding any of it would mean rebuilding the sysroot, which is what Emscripten exists to
# avoid. So this file only recreates the one CMake target the rest of the tree expects.
#
# The `-pthread` and `-sMEMORY64` flags are ABI flags and are applied to every target in
# `cmake/target.cmake`, which runs before this file.

add_library (Threads::Threads INTERFACE IMPORTED)
set_target_properties (Threads::Threads PROPERTIES INTERFACE_COMPILE_OPTIONS "-pthread")

message (STATUS "Default libraries: provided by the Emscripten sysroot")
