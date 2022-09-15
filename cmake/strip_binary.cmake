macro(clickhouse_strip_binary)
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

   add_custom_command(TARGET ${STRIP_TARGET} POST_BUILD
       COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/lib/debug/bin"
       COMMAND mkdir -p "${STRIP_DESTINATION_DIR}/bin"
       COMMAND cp "${STRIP_BINARY_PATH}" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
       # Splits debug symbols into separate file, leaves the binary untouched:
       COMMAND "${OBJCOPY_PATH}" --only-keep-debug --compress-debug-sections "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}" "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
       COMMAND chmod 0644 "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug"
       # Strips binary, sections '.note' & '.comment' are removed in line with Debian's stripping policy: www.debian.org/doc/debian-policy/ch-files.html, section '.clickhouse.hash' is needed for integrity check:
       COMMAND "${STRIP_PATH}" --remove-section=.comment --remove-section=.note --keep-section=.clickhouse.hash "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
       # Associate stripped binary with debug symbols:
       COMMAND "${OBJCOPY_PATH}" --add-gnu-debuglink "${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug" "${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET}"
       COMMENT "Stripping clickhouse binary" VERBATIM
   )

   install(PROGRAMS ${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET} DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
   install(FILES ${STRIP_DESTINATION_DIR}/lib/debug/bin/${STRIP_TARGET}.debug DESTINATION ${CMAKE_INSTALL_LIBDIR}/debug/${CMAKE_INSTALL_FULL_BINDIR} COMPONENT clickhouse)
endmacro()


macro(clickhouse_make_empty_debug_info_for_nfpm)
   set(oneValueArgs TARGET DESTINATION_DIR)
   cmake_parse_arguments(EMPTY_DEBUG "" "${oneValueArgs}" "" ${ARGN})

   if (NOT DEFINED EMPTY_DEBUG_TARGET)
       message(FATAL_ERROR "A target name must be provided for stripping binary")
   endif()

   if (NOT DEFINED EMPTY_DEBUG_DESTINATION_DIR)
       message(FATAL_ERROR "Destination directory for empty debug must be provided")
   endif()

   add_custom_command(TARGET ${EMPTY_DEBUG_TARGET} POST_BUILD
       COMMAND mkdir -p "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug"
       COMMAND touch "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug/${EMPTY_DEBUG_TARGET}.debug"
       COMMENT "Adding empty debug info for NFPM" VERBATIM
   )

   install(FILES "${EMPTY_DEBUG_DESTINATION_DIR}/lib/debug/${EMPTY_DEBUG_TARGET}.debug" DESTINATION "${CMAKE_INSTALL_LIBDIR}/debug/${CMAKE_INSTALL_FULL_BINDIR}" COMPONENT clickhouse)
endmacro()
