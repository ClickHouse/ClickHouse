set(FDB_VERSION 7.1.31)
set(FDB_API_VERSION 710)

if(OS_LINUX)
    set(FOUNDATIONDB_SOURCE_PATCHS
        fdb_delay.patch
        portable-library.patch
        fix-harmful-symbol.patch
    )
    list(TRANSFORM FOUNDATIONDB_SOURCE_PATCHS PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/)

    include(ExternalProject)
    ExternalProject_Add(
        foundationdb_ep
        GIT_REPOSITORY https://github.com/apple/foundationdb.git
        GIT_TAG ${FDB_VERSION}
        UPDATE_COMMAND ""
        PATCH_COMMAND git clean -dffx && git restore . && git apply ${FOUNDATIONDB_SOURCE_PATCHS}
        CMAKE_ARGS
            -DUSE_JEMALLOC=OFF # temporarily disabled because USE_JEMALLOC is buggy
            -DUSE_CCACHE=ON
            -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libfdb_c.so
        LOG_CONFIGURE ON
        LOG_BUILD ON
    )
    ExternalProject_Add_StepDependencies(foundationdb_ep patch ${FOUNDATIONDB_SOURCE_PATCHS})
    # fdb always compile with debug symbols
    ExternalProject_Add_Step(foundationdb_ep strip
        COMMAND strip <INSTALL_DIR>/lib/libfdb_c.so
        DEPENDEES build
    )
    ExternalProject_Get_Property(foundationdb_ep INSTALL_DIR)

    clickhouse_embed_binaries(TARGET _fdb_c_data RESOURCE_DIR ${INSTALL_DIR}/lib RESOURCES libfdb_c.so)
    add_dependencies(_fdb_c_data foundationdb_ep)
    target_link_libraries(_fdb_c_data INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:_fdb_c_data> -Wl,${NO_WHOLE_ARCHIVE}")
else()
    add_library(_fdb_c_data INTERFACE)
endif()

target_compile_definitions(_fdb_c_data INTERFACE FDB_C_RESOURCE_NAME=\"libfdb_c_so\")

add_library(_fdb_c_embed fdb_c.cpp)
target_link_libraries(_fdb_c_embed
    PRIVATE clickhouse_common_io _fdb_c_data
)
target_include_directories(_fdb_c_embed PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_definitions(_fdb_c_embed PUBLIC FDB_API_VERSION=${FDB_API_VERSION})

add_library(ch_contrib::fdb_c_embed ALIAS _fdb_c_embed)
add_library(fdb_c ALIAS _fdb_c_embed)