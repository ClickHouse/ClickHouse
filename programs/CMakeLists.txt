if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

include(${ClickHouse_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

# The `clickhouse` binary is a multi purpose tool that contains multiple execution modes (client, server, etc.),
# each of them may be built and linked as a separate library.
# If you do not know what modes you need, turn this option OFF and enable SERVER and CLIENT only.
if (USE_MUSL)
    # Only clickhouse-keeper can be built with musl currently
    option (ENABLE_CLICKHOUSE_ALL "Enable all ClickHouse modes by default" OFF)
else ()
    option (ENABLE_CLICKHOUSE_ALL "Enable all ClickHouse modes by default" ON)
endif ()

option (ENABLE_CLICKHOUSE_SERVER "Server mode (main mode)" ${ENABLE_CLICKHOUSE_ALL})
option (ENABLE_CLICKHOUSE_CLIENT "Client mode (interactive tui/shell that connects to the server)"
    ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_SELF_EXTRACTING "Self-extracting executable" ON)

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/
option (ENABLE_CLICKHOUSE_LOCAL "Local files fast processing mode" ${ENABLE_CLICKHOUSE_ALL})

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-benchmark/
option (ENABLE_CLICKHOUSE_BENCHMARK "Queries benchmarking mode" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG "Configs processor (extract values etc.)" ${ENABLE_CLICKHOUSE_ALL})

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-compressor/
option (ENABLE_CLICKHOUSE_COMPRESSOR "Data compressor and decompressor" ${ENABLE_CLICKHOUSE_ALL})

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-copier/
option (ENABLE_CLICKHOUSE_COPIER "Inter-cluster data copying mode" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_FORMAT "Queries pretty-printer and formatter with syntax highlighting"
    ${ENABLE_CLICKHOUSE_ALL})

# https://clickhouse.com/docs/en/operations/utilities/clickhouse-obfuscator/
option (ENABLE_CLICKHOUSE_OBFUSCATOR "Table data obfuscator (convert real data to benchmark-ready one)"
    ${ENABLE_CLICKHOUSE_ALL})

# https://clickhouse.com/docs/en/operations/utilities/odbc-bridge/
# TODO Also needs NANODBC.
if (ENABLE_ODBC AND NOT USE_MUSL)
    option (ENABLE_CLICKHOUSE_ODBC_BRIDGE "HTTP-server working like a proxy to ODBC driver" ${ENABLE_CLICKHOUSE_ALL})
else ()
    option (ENABLE_CLICKHOUSE_ODBC_BRIDGE "HTTP-server working like a proxy to ODBC driver" OFF)
endif ()

if (NOT USE_MUSL)
    option (ENABLE_CLICKHOUSE_LIBRARY_BRIDGE "HTTP-server working like a proxy to Library dictionary source" ${ENABLE_CLICKHOUSE_ALL})
endif ()

# https://presentations.clickhouse.com/matemarketing_2020/
option (ENABLE_CLICKHOUSE_GIT_IMPORT "A tool to analyze Git repositories" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER "A tool to export table data files to be later put to a static files web server" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_KEEPER "ClickHouse alternative to ZooKeeper" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_KEEPER_CONVERTER "Util allows to convert ZooKeeper logs and snapshots into clickhouse-keeper snapshot" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_SU "A tool similar to 'su'" ${ENABLE_CLICKHOUSE_ALL})

option (ENABLE_CLICKHOUSE_DISKS "A tool to manage disks" ${ENABLE_CLICKHOUSE_ALL})

if (NOT ENABLE_NURAFT)
    # RECONFIGURE_MESSAGE_LEVEL should not be used here,
    # since ENABLE_NURAFT is set to OFF for FreeBSD and Darwin.
    message (STATUS "clickhouse-keeper and clickhouse-keeper-converter will not be built (lack of NuRaft)")
    set(ENABLE_CLICKHOUSE_KEEPER OFF)
    set(ENABLE_CLICKHOUSE_KEEPER_CONVERTER OFF)
endif()

option(ENABLE_CLICKHOUSE_INSTALL "Install ClickHouse without .deb/.rpm/.tgz packages (having the binary only)" ${ENABLE_CLICKHOUSE_ALL})

message(STATUS "ClickHouse modes:")

if (NOT ENABLE_CLICKHOUSE_SERVER)
    message(WARNING "ClickHouse server mode is not going to be built.")
else()
    message(STATUS "Server mode: ON")
endif()

if (NOT ENABLE_CLICKHOUSE_CLIENT)
    message(WARNING "ClickHouse client mode is not going to be built. You won't be able to connect to the server and run tests")
else()
    message(STATUS "Client mode: ON")
endif()

if (ENABLE_CLICKHOUSE_LOCAL)
    message(STATUS "Local mode: ON")
else()
    message(STATUS "Local mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    message(STATUS "Self-extracting executable: ON")
else()
    message(STATUS "Self-extracting executable: OFF")
endif()

if (ENABLE_CLICKHOUSE_BENCHMARK)
    message(STATUS "Benchmark mode: ON")
else()
    message(STATUS "Benchmark mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
    message(STATUS "Extract from config mode: ON")
else()
    message(STATUS "Extract from config mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_COMPRESSOR)
    message(STATUS "Compressor mode: ON")
else()
    message(STATUS "Compressor mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_COPIER)
    message(STATUS "Copier mode: ON")
else()
    message(STATUS "Copier mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_FORMAT)
    message(STATUS "Format mode: ON")
else()
    message(STATUS "Format mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_OBFUSCATOR)
    message(STATUS "Obfuscator mode: ON")
else()
    message(STATUS "Obfuscator mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_ODBC_BRIDGE)
    message(STATUS "ODBC bridge mode: ON")
else()
    message(STATUS "ODBC bridge mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_LIBRARY_BRIDGE)
    message(STATUS "Library bridge mode: ON")
else()
    message(STATUS "Library bridge mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_INSTALL)
    message(STATUS "ClickHouse install: ON")
else()
    message(STATUS "ClickHouse install: OFF")
endif()

if (ENABLE_CLICKHOUSE_GIT_IMPORT)
    message(STATUS "ClickHouse git-import: ON")
else()
    message(STATUS "ClickHouse git-import: OFF")
endif()

if (ENABLE_CLICKHOUSE_KEEPER)
    message(STATUS "ClickHouse keeper mode: ON")
else()
    message(STATUS "ClickHouse keeper mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_KEEPER_CONVERTER)
    message(STATUS "ClickHouse keeper-converter mode: ON")
else()
    message(STATUS "ClickHouse keeper-converter mode: OFF")
endif()

if(NOT (USE_STATIC_LIBRARIES OR SPLIT_SHARED_LIBRARIES))
    set(CLICKHOUSE_ONE_SHARED ON)
endif()

if (ENABLE_CLICKHOUSE_DISKS)
    message(STATUS "Clickhouse disks mode: ON")
else()
    message(STATUS "ClickHouse disks mode: OFF")
endif()

if (ENABLE_CLICKHOUSE_SU)
    message(STATUS "ClickHouse su: ON")
else()
    message(STATUS "ClickHouse su: OFF")
endif()

configure_file (config_tools.h.in ${ConfigIncludePath}/config_tools.h)

macro(clickhouse_target_link_split_lib target name)
    if(NOT CLICKHOUSE_ONE_SHARED)
        target_link_libraries(${target} PRIVATE clickhouse-${name}-lib)
    else()
        target_link_libraries(${target} PRIVATE clickhouse-lib)
    endif()
endmacro()

macro(clickhouse_program_add_library name)
    string(TOUPPER ${name} name_uc)
    string(REPLACE "-" "_" name_uc ${name_uc})

    # Some dark magic
    set(CLICKHOUSE_${name_uc}_SOURCES ${CLICKHOUSE_${name_uc}_SOURCES} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_LINK ${CLICKHOUSE_${name_uc}_LINK} PARENT_SCOPE)
    set(CLICKHOUSE_${name_uc}_INCLUDE ${CLICKHOUSE_${name_uc}_INCLUDE} PARENT_SCOPE)

    if(NOT CLICKHOUSE_ONE_SHARED)
        add_library(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_SOURCES})

        set(_link ${CLICKHOUSE_${name_uc}_LINK}) # can't use ${} in if()
        if(_link)
            target_link_libraries(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_LINK})
        endif()

        set(_include ${CLICKHOUSE_${name_uc}_INCLUDE}) # can't use ${} in if()
        if (_include)
            target_include_directories(clickhouse-${name}-lib ${CLICKHOUSE_${name_uc}_INCLUDE})
        endif()
    endif()
endmacro()

macro(clickhouse_program_add name)
    clickhouse_program_add_library(${name})
endmacro()

add_subdirectory (server)
add_subdirectory (client)
add_subdirectory (local)
add_subdirectory (benchmark)
add_subdirectory (extract-from-config)
add_subdirectory (compressor)
add_subdirectory (copier)
add_subdirectory (format)
add_subdirectory (obfuscator)
add_subdirectory (install)
add_subdirectory (git-import)
add_subdirectory (bash-completion)
add_subdirectory (static-files-disk-uploader)
add_subdirectory (su)
add_subdirectory (disks)

if (ENABLE_CLICKHOUSE_KEEPER)
    add_subdirectory (keeper)
endif()

if (ENABLE_CLICKHOUSE_KEEPER_CONVERTER)
    add_subdirectory (keeper-converter)
endif()

if (ENABLE_CLICKHOUSE_ODBC_BRIDGE)
    add_subdirectory (odbc-bridge)
endif ()

if (ENABLE_CLICKHOUSE_LIBRARY_BRIDGE)
    add_subdirectory (library-bridge)
endif ()

if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    add_subdirectory (self-extracting)
endif ()

if (CLICKHOUSE_ONE_SHARED)
  add_library(clickhouse-lib SHARED
    ${CLICKHOUSE_SERVER_SOURCES}
    ${CLICKHOUSE_CLIENT_SOURCES}
    ${CLICKHOUSE_LOCAL_SOURCES}
    ${CLICKHOUSE_BENCHMARK_SOURCES}
    ${CLICKHOUSE_COPIER_SOURCES}
    ${CLICKHOUSE_EXTRACT_FROM_CONFIG_SOURCES}
    ${CLICKHOUSE_COMPRESSOR_SOURCES}
    ${CLICKHOUSE_FORMAT_SOURCES}
    ${CLICKHOUSE_OBFUSCATOR_SOURCES}
    ${CLICKHOUSE_GIT_IMPORT_SOURCES}
    ${CLICKHOUSE_ODBC_BRIDGE_SOURCES}
    ${CLICKHOUSE_KEEPER_SOURCES}
    ${CLICKHOUSE_KEEPER_CONVERTER_SOURCES}
    ${CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_SOURCES}
    ${CLICKHOUSE_SU_SOURCES})

    target_link_libraries(clickhouse-lib
      ${CLICKHOUSE_SERVER_LINK}
      ${CLICKHOUSE_CLIENT_LINK}
      ${CLICKHOUSE_LOCAL_LINK}
      ${CLICKHOUSE_BENCHMARK_LINK}
      ${CLICKHOUSE_COPIER_LINK}
      ${CLICKHOUSE_EXTRACT_FROM_CONFIG_LINK}
      ${CLICKHOUSE_COMPRESSOR_LINK}
      ${CLICKHOUSE_FORMAT_LINK}
      ${CLICKHOUSE_OBFUSCATOR_LINK}
      ${CLICKHOUSE_GIT_IMPORT_LINK}
      ${CLICKHOUSE_ODBC_BRIDGE_LINK}
      ${CLICKHOUSE_KEEPER_LINK}
      ${CLICKHOUSE_KEEPER_CONVERTER_LINK}
      ${CLICKHOUSE_STATIC_FILES_DISK_UPLOADER_LINK}
      ${CLICKHOUSE_SU_LINK})

    target_include_directories(clickhouse-lib
      ${CLICKHOUSE_SERVER_INCLUDE}
      ${CLICKHOUSE_CLIENT_INCLUDE}
      ${CLICKHOUSE_LOCAL_INCLUDE}
      ${CLICKHOUSE_BENCHMARK_INCLUDE}
      ${CLICKHOUSE_COPIER_INCLUDE}
      ${CLICKHOUSE_EXTRACT_FROM_CONFIG_INCLUDE}
      ${CLICKHOUSE_COMPRESSOR_INCLUDE}
      ${CLICKHOUSE_FORMAT_INCLUDE}
      ${CLICKHOUSE_OBFUSCATOR_INCLUDE}
      ${CLICKHOUSE_GIT_IMPORT_INCLUDE}
      ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE}
      ${CLICKHOUSE_KEEPER_INCLUDE}
      ${CLICKHOUSE_KEEPER_CONVERTER_INCLUDE})

    set_target_properties(clickhouse-lib PROPERTIES SOVERSION ${VERSION_MAJOR}.${VERSION_MINOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse DEBUG_POSTFIX "")
    install (TARGETS clickhouse-lib LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endif()

clickhouse_add_executable (clickhouse main.cpp)

if (NOT USE_STATIC_LIBRARIES AND SPLIT_SHARED_LIBRARIES)
    # Shared split (dev) build: In CI, the server is run with custom LD_LIBRARY_PATH. This makes the harmful env check re-execute the
    # process in a clean environment but as in CI the containing directory is not included in DT_RUNPATH/DT_RPATH, the server won't come up.
    target_compile_definitions(clickhouse PRIVATE DISABLE_HARMFUL_ENV_VAR_CHECK)
endif ()

# A library that prevent usage of several functions from libc.
if (ARCH_AMD64 AND OS_LINUX AND NOT OS_ANDROID)
    set (HARMFUL_LIB harmful)
endif ()

target_link_libraries (clickhouse PRIVATE clickhouse_common_io string_utils ${HARMFUL_LIB})
target_include_directories (clickhouse PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

if (ENABLE_CLICKHOUSE_SERVER)
    clickhouse_target_link_split_lib(clickhouse server)
endif ()
if (ENABLE_CLICKHOUSE_CLIENT)
    clickhouse_target_link_split_lib(clickhouse client)
endif ()
if (ENABLE_CLICKHOUSE_LOCAL)
    clickhouse_target_link_split_lib(clickhouse local)
endif ()
if (ENABLE_CLICKHOUSE_BENCHMARK)
    clickhouse_target_link_split_lib(clickhouse benchmark)
endif ()
if (ENABLE_CLICKHOUSE_COPIER)
    clickhouse_target_link_split_lib(clickhouse copier)
endif ()
if (ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
    clickhouse_target_link_split_lib(clickhouse extract-from-config)
endif ()
if (ENABLE_CLICKHOUSE_COMPRESSOR)
    clickhouse_target_link_split_lib(clickhouse compressor)
endif ()
if (ENABLE_CLICKHOUSE_FORMAT)
    clickhouse_target_link_split_lib(clickhouse format)
endif ()
if (ENABLE_CLICKHOUSE_OBFUSCATOR)
    clickhouse_target_link_split_lib(clickhouse obfuscator)
endif ()
if (ENABLE_CLICKHOUSE_GIT_IMPORT)
    clickhouse_target_link_split_lib(clickhouse git-import)
endif ()
if (ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER)
    clickhouse_target_link_split_lib(clickhouse static-files-disk-uploader)
endif ()
if (ENABLE_CLICKHOUSE_SU)
    clickhouse_target_link_split_lib(clickhouse su)
endif ()
if (ENABLE_CLICKHOUSE_KEEPER)
    clickhouse_target_link_split_lib(clickhouse keeper)
endif()
if (ENABLE_CLICKHOUSE_KEEPER_CONVERTER)
    clickhouse_target_link_split_lib(clickhouse keeper-converter)
endif()
if (ENABLE_CLICKHOUSE_INSTALL)
    clickhouse_target_link_split_lib(clickhouse install)
endif ()
if (ENABLE_CLICKHOUSE_DISKS)
    clickhouse_target_link_split_lib(clickhouse disks)
endif ()

set (CLICKHOUSE_BUNDLE)
if (ENABLE_CLICKHOUSE_SELF_EXTRACTING)
    list(APPEND CLICKHOUSE_BUNDLE self-extracting)
endif ()
if (ENABLE_CLICKHOUSE_SERVER)
    add_custom_target (clickhouse-server ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-server DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-server" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-server)
endif ()
if (ENABLE_CLICKHOUSE_CLIENT)
    add_custom_target (clickhouse-client ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-client DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-client" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-client)
endif ()
if (ENABLE_CLICKHOUSE_LOCAL)
    add_custom_target (clickhouse-local ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-local DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-local" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-local)
endif ()
if (ENABLE_CLICKHOUSE_BENCHMARK)
    add_custom_target (clickhouse-benchmark ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-benchmark DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-benchmark" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-benchmark)
endif ()
if (ENABLE_CLICKHOUSE_COPIER)
    add_custom_target (clickhouse-copier ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-copier DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-copier" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-copier)
endif ()
if (ENABLE_CLICKHOUSE_EXTRACT_FROM_CONFIG)
    add_custom_target (clickhouse-extract-from-config ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-extract-from-config DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-extract-from-config" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-extract-from-config)
endif ()
if (ENABLE_CLICKHOUSE_COMPRESSOR)
    add_custom_target (clickhouse-compressor ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-compressor DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-compressor" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-compressor)
endif ()
if (ENABLE_CLICKHOUSE_FORMAT)
    add_custom_target (clickhouse-format ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-format DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-format" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-format)
endif ()
if (ENABLE_CLICKHOUSE_OBFUSCATOR)
    add_custom_target (clickhouse-obfuscator ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-obfuscator DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-obfuscator" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-obfuscator)
endif ()
if (ENABLE_CLICKHOUSE_GIT_IMPORT)
    add_custom_target (clickhouse-git-import ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-git-import DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-git-import" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-git-import)
endif ()
if (ENABLE_CLICKHOUSE_STATIC_FILES_DISK_UPLOADER)
    add_custom_target (clickhouse-static-files-disk-uploader ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-static-files-disk-uploader DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-static-files-disk-uploader" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-static-files-disk-uploader)
endif ()
if (ENABLE_CLICKHOUSE_SU)
    add_custom_target (clickhouse-su ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-su DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-su" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-su)
endif ()

if (ENABLE_CLICKHOUSE_KEEPER)
    if (NOT BUILD_STANDALONE_KEEPER AND CREATE_KEEPER_SYMLINK)
        add_custom_target (clickhouse-keeper ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-keeper DEPENDS clickhouse)
        install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-keeper" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    endif()

    # otherwise we don't build keeper
    if (BUILD_STANDALONE_KEEPER OR CREATE_KEEPER_SYMLINK)
        list(APPEND CLICKHOUSE_BUNDLE clickhouse-keeper)
    endif()
endif ()
if (ENABLE_CLICKHOUSE_KEEPER_CONVERTER)
    add_custom_target (clickhouse-keeper-converter ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-keeper-converter DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-keeper-converter" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-keeper-converter)
endif ()
if (ENABLE_CLICKHOUSE_DISKS)
    add_custom_target (clickhouse-disks ALL COMMAND ${CMAKE_COMMAND} -E create_symlink clickhouse clickhouse-disks DEPENDS clickhouse)
    install (FILES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse-disks" DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    list(APPEND CLICKHOUSE_BUNDLE clickhouse-disks)
endif ()

add_custom_target (clickhouse-bundle ALL DEPENDS ${CLICKHOUSE_BUNDLE})

if (USE_GDB_ADD_INDEX)
    add_custom_command(TARGET clickhouse POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} clickhouse COMMENT "Adding .gdb-index to clickhouse" VERBATIM)
endif()

if (USE_BINARY_HASH)
    add_custom_command(TARGET clickhouse POST_BUILD COMMAND ./clickhouse hash-binary > hash && ${OBJCOPY_PATH} --add-section .clickhouse.hash=hash clickhouse COMMENT "Adding section '.clickhouse.hash' to clickhouse binary" VERBATIM)
endif()

if (SPLIT_DEBUG_SYMBOLS)
    clickhouse_split_debug_symbols(TARGET clickhouse DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH clickhouse)
else()
    clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPLITTED_DEBUG_SYMBOLS_DIR})
    install (TARGETS clickhouse RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif()

if (ENABLE_TESTS)
    set (CLICKHOUSE_UNIT_TESTS_TARGETS unit_tests_dbms)
    add_custom_target (clickhouse-tests ALL DEPENDS ${CLICKHOUSE_UNIT_TESTS_TARGETS})
    add_dependencies(clickhouse-bundle clickhouse-tests)
endif()

if (ENABLE_FUZZING)
    add_compile_definitions(FUZZING_MODE=1)
endif ()
