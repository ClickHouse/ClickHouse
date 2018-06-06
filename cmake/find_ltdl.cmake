set (LTDL_PATHS "/usr/local/opt/libtool/lib")
find_library (LTDL_LIBRARY ltdl PATHS ${LTDL_PATHS})
message (STATUS "Using ltdl: ${LTDL_LIBRARY}")
