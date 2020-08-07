option (USE_INTERNAL_CONSISTENT_HASHING_LIBRARY "Set to FALSE to use consistent-hashing library from Arcadia (Yandex internal repository) instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_CONSISTENT_HASHING_LIBRARY)
    find_library (CONSISTENT_HASHING_LIBRARY consistent-hashing)
    find_path (CONSISTENT_HASHING_INCLUDE_DIR NAMES consistent_hashing.h PATHS ${CONSISTENT_HASHING_INCLUDE_PATHS})
endif ()

if (CONSISTENT_HASHING_LIBRARY AND CONSISTENT_HASHING_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_CONSISTENT_HASHING_LIBRARY 1)
    set (CONSISTENT_HASHING_LIBRARY consistent-hashing)
endif ()

message (STATUS "Using consistent-hashing: ${CONSISTENT_HASHING_INCLUDE_DIR} : ${CONSISTENT_HASHING_LIBRARY}")
