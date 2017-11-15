set (LTDL_PATHS "/usr/local/opt/libtool/lib")
find_library (LTDL_LIB ltdl PATHS ${LTDL_PATHS})
message (STATUS "Using ltdl: ${LTDL_LIB}")
