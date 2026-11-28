# The debug sections of the separate files are compressed: nfpm cannot package files over 4 GiB into rpm.
# zlib is the fallback for an objcopy built without zstd.
if (SPLIT_DEBUG_SYMBOLS)
    set (SPLIT_DEBUG_COMPRESSION_PROBE "${CMAKE_CURRENT_BINARY_DIR}/objcopy_zstd_probe")
    execute_process (
        COMMAND "${OBJCOPY_PATH}" --compress-debug-sections=zstd "${OBJCOPY_PATH}" "${SPLIT_DEBUG_COMPRESSION_PROBE}"
        RESULT_VARIABLE OBJCOPY_ZSTD_RESULT
        OUTPUT_QUIET ERROR_QUIET)
    file (REMOVE "${SPLIT_DEBUG_COMPRESSION_PROBE}")
    if (OBJCOPY_ZSTD_RESULT EQUAL 0)
        set (SPLIT_DEBUG_COMPRESSION "zstd")
    else ()
        set (SPLIT_DEBUG_COMPRESSION "zlib")
    endif ()
    message (STATUS "Compressing debug sections of split debug symbols with ${SPLIT_DEBUG_COMPRESSION}")
endif ()

# Generates a separate file with debug symbols while stripping it from the main binary.
# This is needed for Debian packages.
macro(clickhouse_split_debug_symbols)
   set(oneValueArgs TARGET DESTINATION_DIR BINARY_PATH)

   cmake_parse_arguments(STRIP "" "${oneValueArgs}" "" ${ARGN})

   if (NOT DEFINED STRIP_TARGET)
       message(FATAL_ERROR "A target name must be provided for stripping binary")
   endif()

   if (NOT DEFINED STRIP_BINARY_PATH)
       message(FATAL_ERROR "A binary path name must be provided for stripping binary")
   endif()

   if (NOT DEFINED STRIP_DESTINATION_DIR)
       message(FATAL_ERROR "Destination directory for stripped binary must be provided")
   endif()

   # Commands that write the contents of the new sections, then the arguments of the single `objcopy` invocation that adds them.
   set(SYMBOL_SECTIONS_COMMANDS)
   set(SYMBOL_SECTIONS_OBJCOPY_ARGS)
   set(SYMBOL_SECTIONS_TEMPORARY_FILES)
   if (ADD_COMPACT_SYMBOLS_SECTION)
       # Use the native-build tool only when actually cross-compiling (host != target), matching the
       # native-build guard in the root CMakeLists.txt and utils/compact-symbols/CMakeLists.txt.
       if (NOT(
           CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME
           AND CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR
       ))
           set(COMPACT_SYMBOLS_TOOL "${PROJECT_BINARY_DIR}/native/utils/compact-symbols/compact-symbols")
       else()
           set(COMPACT_SYMBOLS_TOOL "$<TARGET_FILE:compact-symbols>")
           # The tool target is declared after programs, so add the dependency after the top-level target graph is complete.
           cmake_language(EVAL CODE "
               cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]]
                   CALL add_dependencies [[${STRIP_TARGET}]] compact-symbols)
           ")
       endif()

       set(COMPACT_SYMBOLS_BLOB "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}.symbols")
       # `--strip-debug` removes no symbol that `compact-symbols` keeps (it keeps symbols with an address and a size).
       list(APPEND SYMBOL_SECTIONS_COMMANDS
           COMMAND "${COMPACT_SYMBOLS_TOOL}" "${STRIP_BINARY_PATH}" "${COMPACT_SYMBOLS_BLOB}"
       )
       list(APPEND SYMBOL_SECTIONS_OBJCOPY_ARGS "--add-section=.clickhouse.symbols=${COMPACT_SYMBOLS_BLOB}")
       list(APPEND SYMBOL_SECTIONS_TEMPORARY_FILES "${COMPACT_SYMBOLS_BLOB}")
   endif()

   if (ADD_GNU_DEBUGDATA_SECTION)
       set(MINI_DEBUGINFO_PATH "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.mini_debuginfo")
       list(APPEND SYMBOL_SECTIONS_COMMANDS
           COMMAND "${OBJCOPY_PATH}" --strip-debug "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${MINI_DEBUGINFO_PATH}"
           # Compress 16 MiB blocks in parallel; the output depends on the block size, not on the number of threads.
           COMMAND "${XZ_PATH}" -9 -T0 --block-size=16MiB -f "${MINI_DEBUGINFO_PATH}"
       )
       list(APPEND SYMBOL_SECTIONS_OBJCOPY_ARGS "--add-section=.gnu_debugdata=${MINI_DEBUGINFO_PATH}.xz")
       list(APPEND SYMBOL_SECTIONS_TEMPORARY_FILES "${MINI_DEBUGINFO_PATH}" "${MINI_DEBUGINFO_PATH}.xz")
   endif()

   set(SYMBOL_SECTIONS_CLEANUP)
   if (ADD_COMPACT_SYMBOLS_SECTION OR ADD_GNU_DEBUGDATA_SECTION)
       # GNU `objcopy` regenerates `.symtab` and `.strtab` on `--remove-section` while the symbols are still there, so drop them with `--strip-unneeded`.
       # Not `--strip-all`: `llvm-objcopy` would also remove the non-allocated `.clickhouse.hash` and `.gnu_debuglink` sections.
       list(APPEND SYMBOL_SECTIONS_OBJCOPY_ARGS --strip-unneeded "--remove-section=.symtab" "--remove-section=.strtab")
       set(SYMBOL_SECTIONS_CLEANUP COMMAND "${CMAKE_COMMAND}" -E rm -f ${SYMBOL_SECTIONS_TEMPORARY_FILES})
   endif()

   add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
       COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
       COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
       # Splits debug symbols into separate file, leaves the binary untouched.
       COMMAND "${OBJCOPY_PATH}" --only-keep-debug "--compress-debug-sections=${SPLIT_DEBUG_COMPRESSION}" "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
       COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
       ${SYMBOL_SECTIONS_COMMANDS}
       # Strips binary, sections '.note' & '.comment' are removed in line with Debian's stripping policy: www.debian.org/doc/debian-policy/ch-files.html, section '.clickhouse.hash' is needed for integrity check.
       # Also, after we disabled the export of symbols for dynamic linking, we still to keep a static symbol table for good stack traces.
       # Strip, add `.gnu_debuglink` and the symbol sections in one pass: each `objcopy` pass rewrites the whole binary.
       COMMAND "${OBJCOPY_PATH}" --strip-debug --remove-section=.comment --remove-section=.note
           "--add-gnu-debuglink=${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
           ${SYMBOL_SECTIONS_OBJCOPY_ARGS}
           "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
       ${SYMBOL_SECTIONS_CLEANUP}
       COMMENT "Stripping clickhouse binary" VERBATIM
   )

   unset(SYMBOL_SECTIONS_COMMANDS)
   unset(SYMBOL_SECTIONS_OBJCOPY_ARGS)
   unset(SYMBOL_SECTIONS_TEMPORARY_FILES)
   unset(SYMBOL_SECTIONS_CLEANUP)
   unset(COMPACT_SYMBOLS_TOOL)
   unset(COMPACT_SYMBOLS_BLOB)
   unset(MINI_DEBUGINFO_PATH)
endmacro()
