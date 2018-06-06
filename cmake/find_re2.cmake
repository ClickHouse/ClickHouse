option (USE_INTERNAL_RE2_LIBRARY "Set to FALSE to use system re2 library instead of bundled [slower]" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_RE2_LIBRARY)
    find_library (RE2_LIBRARY re2)
    find_path (RE2_INCLUDE_DIR NAMES re2/re2.h PATHS ${RE2_INCLUDE_PATHS})
endif ()

if (RE2_LIBRARY AND RE2_INCLUDE_DIR)
    set (RE2_ST_LIBRARY ${RE2_LIBRARY})
else ()
    set (USE_INTERNAL_RE2_LIBRARY 1)
    set (RE2_LIBRARY re2)
    set (RE2_ST_LIBRARY re2_st)
    set (USE_RE2_ST 1)
endif ()

message (STATUS "Using re2: ${RE2_INCLUDE_DIR} : ${RE2_LIBRARY}; ${RE2_ST_INCLUDE_DIR} : ${RE2_ST_LIBRARY}")
