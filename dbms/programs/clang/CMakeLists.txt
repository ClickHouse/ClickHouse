if (USE_EMBEDDED_COMPILER)
    add_subdirectory ("Compiler-${LLVM_VERSION}")
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    if (USE_EMBEDDED_COMPILER)
        link_directories (${LLVM_LIBRARY_DIRS})
        add_executable (clickhouse-clang clickhouse-clang.cpp)
        target_link_libraries (clickhouse-clang clickhouse-compiler-lib)
        add_executable (clickhouse-lld clickhouse-lld.cpp)
        target_link_libraries (clickhouse-lld clickhouse-compiler-lib)
        install (TARGETS clickhouse-clang clickhouse-lld RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif ()
endif ()

set (TMP_HEADERS_DIR "${CMAKE_CURRENT_BINARY_DIR}/${INTERNAL_COMPILER_HEADERS_RELATIVE}")
# Make and install empty dir for debian package if compiler disabled
add_custom_target (make-headers-directory ALL COMMAND ${CMAKE_COMMAND} -E make_directory ${TMP_HEADERS_DIR})
install (DIRECTORY ${TMP_HEADERS_DIR} DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse/${INTERNAL_COMPILER_HEADERS_DIR} COMPONENT clickhouse)
# TODO: fix on macos copy_headers.sh: sed --posix

if (USE_EMBEDDED_COMPILER)
    set (COPY_HEADERS_COMPILER "${CMAKE_CURRENT_BINARY_DIR}/../${INTERNAL_COMPILER_EXECUTABLE}")
    set (COPY_HEADERS_DEPENDS clickhouse-clang)
elseif (EXISTS ${INTERNAL_COMPILER_BIN_ROOT}${INTERNAL_COMPILER_EXECUTABLE})
    set (COPY_HEADERS_COMPILER "${INTERNAL_COMPILER_BIN_ROOT}${INTERNAL_COMPILER_EXECUTABLE}")
endif ()

if (COPY_HEADERS_COMPILER AND NOT APPLE)
    add_custom_target (copy-headers ALL env CLANG=${COPY_HEADERS_COMPILER} BUILD_PATH=${ClickHouse_BINARY_DIR} DESTDIR=${ClickHouse_SOURCE_DIR} ${ClickHouse_SOURCE_DIR}/copy_headers.sh ${ClickHouse_SOURCE_DIR} ${TMP_HEADERS_DIR} DEPENDS ${COPY_HEADERS_DEPENDS} WORKING_DIRECTORY ${ClickHouse_SOURCE_DIR} SOURCES ${ClickHouse_SOURCE_DIR}/copy_headers.sh)

    if (USE_INTERNAL_LLVM_LIBRARY)
        set (CLANG_HEADERS_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm/clang/lib/Headers")
        set (CLANG_HEADERS_DEST "${TMP_HEADERS_DIR}/usr/local/lib/clang/${LLVM_VERSION}/include") # original: ${LLVM_LIBRARY_OUTPUT_INTDIR}/clang/${CLANG_VERSION}/include
        add_custom_target (copy-headers-clang ALL ${CMAKE_COMMAND} -E make_directory ${CLANG_HEADERS_DEST} && ${CMAKE_COMMAND} -E copy_if_different ${CLANG_HEADERS_DIR}/* ${CLANG_HEADERS_DEST} )
        add_dependencies (copy-headers copy-headers-clang)
    endif ()
endif ()
