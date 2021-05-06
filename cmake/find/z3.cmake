find_library(Z3_LIBRARY z3)
find_path (Z3_INCLUDE_DIR NAMES z3++.h PATHS ${Z3_INCLUDE_PATHS})

set (USE_Z3 1)
message (STATUS "Using Z3=${USE_Z3}: ${Z3_INCLUDE_DIR} : ${Z3_LIBRARY}")