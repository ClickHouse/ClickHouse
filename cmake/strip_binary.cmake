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
     COMMAND bash ${ClickHouse_SOURCE_DIR}/cmake/strip.sh ${STRIP_BINARY_PATH} ${STRIP_DESTINATION_DIR} ${OBJCOPY_PATH} ${READELF_PATH}
     COMMENT "Stripping clickhouse binary" VERBATIM
   )

   install(PROGRAMS ${STRIP_DESTINATION_DIR}/bin/${STRIP_TARGET} DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
   install(DIRECTORY ${STRIP_DESTINATION_DIR}/lib/debug DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT clickhouse)
endmacro()
