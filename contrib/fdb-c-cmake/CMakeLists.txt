set(FDB_VERSION 7.1.31)
set(FDB_API_VERSION 710)

if(OS_LINUX)
    set(FOUNDATIONDB_SOURCE_PATCHS
        inject-fdb-c-extensions.patch
        only-build-client.patch
        fix-harmful-symbol.patch
        fix-cmake-flow-target.patch
        backport-doctest-2.4.8.patch
        fix-missing-ftw.patch
        disable-s3.patch
        bundle-llvm-runtimes.patch
        fix-jemalloc-dlopen.patch
    )
    list(TRANSFORM FOUNDATIONDB_SOURCE_PATCHS PREPEND ${CMAKE_CURRENT_SOURCE_DIR}/)
    set(FOUNDATIONDB_C_EXTENSION_SOURCE fdb_c.actor.cpp)

    set(FOUNDATIONDB_CMAKE_ARGS
        -DUSE_CCACHE=ON
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DDISABLE_TLS=ON # FIXME
    )
    if(SANITIZE STREQUAL "memory")
        list(APPEND FOUNDATIONDB_CMAKE_ARGS
            -DUSE_MSAN=ON
            # FIXME: msan for dynamic tls
            # MSAN reports false-positive with dynamic TLS. So we have to use
            # static tls for dlopen libraries.
            # See alse:
            #   * https://github.com/llvm/llvm-project/issues/52872
            #
            # However, fdb's tls are too big. You have to increase the amount
            # of surplus static TLS to make dlopen work, which requires glibc >= 2.33:
            #   export GLIBC_TUNABLES=glibc.rtld.optional_static_tls=10240
            # Otherwise, dlopen will failed with `cannot allocate memory in static TLS block`
            # See also:
            #   * https://maskray.me/blog/2021-02-14-all-about-thread-local-storage
            #   * https://www.gnu.org/software/libc/manual/html_node/Dynamic-Linking-Tunables.html#index-glibc_002ertld_002enns
            
            # Other CXX flags:
            # * `-DMEMORT_SANITIZER` should be defined in fdb's CMakeLists. It's an upstream bug.
            "-DCMAKE_CXX_FLAGS=-ftls-model=initial-exec -DMEMORY_SANITIZER"
        )
    endif()
    
    if(SANITIZE STREQUAL "thread")
        list(APPEND FOUNDATIONDB_CMAKE_ARGS -DUSE_TSAN=ON)
    endif()

    include(ExternalProject)
    ExternalProject_Add(
        foundationdb_ep
        GIT_REPOSITORY https://github.com/apple/foundationdb.git
        GIT_TAG ${FDB_VERSION}
        UPDATE_DISCONNECTED ON
        PATCH_COMMAND
            git clean -dffx &&
            git restore . &&
            git apply ${FOUNDATIONDB_SOURCE_PATCHS} &&
            cp ${CMAKE_CURRENT_SOURCE_DIR}/${FOUNDATIONDB_C_EXTENSION_SOURCE} bindings/c/fdb_c.actor.cpp
        CMAKE_GENERATOR Ninja
        CMAKE_ARGS ${FOUNDATIONDB_CMAKE_ARGS}
        BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target fdb_c
        INSTALL_COMMAND ""
        BUILD_BYPRODUCTS <BINARY_DIR>/lib/libfdb_c.so
        LOG_CONFIGURE ON
        LOG_BUILD ON
    )
    ExternalProject_Add_StepDependencies(foundationdb_ep patch ${FOUNDATIONDB_SOURCE_PATCHS} ${FOUNDATIONDB_C_EXTENSION_SOURCE})

    # fdb always compile with debug symbols
    if(SANITIZE OR CMAKE_BUILD_TYPE STREQUAL "Debug")
        ExternalProject_Add_Step(foundationdb_ep strip
            COMMAND cp <BINARY_DIR>/lib/libfdb_c.so ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so
            DEPENDEES install
            BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so
        )
    else()
        ExternalProject_Add_Step(foundationdb_ep strip
            COMMAND strip <BINARY_DIR>/lib/libfdb_c.so -o ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so
            DEPENDEES install
            BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so
        )
    endif()
    set(FDB_C_RESOURCE_NAME "libfdb_c.so")

    # TODO: Add an option to toggle compression. Compression is not necessary for striped libfdb_c.so (~20MB).
    ExternalProject_Add_Step(foundationdb_ep compress
        COMMAND zstd ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so -f -o ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so.zst
        DEPENDEES strip
        BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/libfdb_c.so.zst
    )
    set(FDB_C_RESOURCE_NAME "libfdb_c.so.zst")

    set(FDB_C_RESOURCE_PATH "${CMAKE_CURRENT_BINARY_DIR}/${FDB_C_RESOURCE_NAME}")

    # Extract & Post process header files
    # 1. Copy ep's include dir to BINARY_DIR
    # 2. Move fdb_c.h => fdb_c_namespaced.h
    # 3. Extract and fix error_definitions.h
    ExternalProject_Add_Step(foundationdb_ep process_headers
        COMMAND
            rm -rf ${CMAKE_CURRENT_BINARY_DIR}/include &&
            mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb &&
            cp <SOURCE_DIR>/bindings/c/foundationdb/fdb_c.h
               <SOURCE_DIR>/bindings/c/foundationdb/fdb_c_types.h
               <SOURCE_DIR>/flow/error_definitions.h
               <BINARY_DIR>/bindings/c/foundationdb/fdb_c_options.g.h
               ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb &&
            mv ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb/fdb_c.h ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb/fdb_c_namespaced.h &&
            sed -i "s/\)$<SEMICOLON>$/)/" ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb/error_definitions.h &&
            sed -i "s/^extern \"C\" \{$/namespace foundationdb::api \{/" ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb/fdb_c_namespaced.h
        DEPENDEES install
        BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb/fdb_c_namespaced.h
    )

    ExternalProject_Add_StepTargets(foundationdb_ep process_headers)

    # fdb-c headers
    add_library(_fdb_c_headers INTERFACE)
    add_dependencies(_fdb_c_headers foundationdb_ep-process_headers)
    target_include_directories(_fdb_c_headers INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/include)
else()
    message(FATAL_ERROR "contrib/fdb-c only support linux now")
endif()

add_library(_fdb_c_embed fdb_c.cpp)
set_source_files_properties(fdb_c.cpp PROPERTIES OBJECT_DEPENDS "${FDB_C_RESOURCE_PATH}")
target_compile_definitions(_fdb_c_embed PRIVATE "FDB_C_RESOURCE_PATH=\"${FDB_C_RESOURCE_PATH}\"")
target_link_libraries(_fdb_c_embed
    PUBLIC _fdb_c_headers
    PRIVATE clickhouse_common_io ch_contrib::incbin
)
target_include_directories(_fdb_c_embed PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_definitions(_fdb_c_embed PUBLIC FDB_API_VERSION=${FDB_API_VERSION})

add_library(ch_contrib::fdb_c_embed ALIAS _fdb_c_embed)
add_library(ch_contrib::fdb_c ALIAS _fdb_c_embed)
add_library(fdb_c ALIAS _fdb_c_embed)