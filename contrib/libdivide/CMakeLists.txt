add_library (_libdivide INTERFACE)
target_include_directories (_libdivide SYSTEM BEFORE INTERFACE .)
add_library (ch_contrib::libdivide ALIAS _libdivide)
