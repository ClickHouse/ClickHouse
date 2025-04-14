add_subdirectory (workspace)

option(ENABLE_CHCACHE "Enable chcache" ${ENABLE_RUST})

if (ENABLE_CHCACHE)
    add_subdirectory (chcache)
endif()
