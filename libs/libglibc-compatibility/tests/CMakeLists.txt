include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

foreach (T longjmp siglongjmp)
    add_executable (${T} ${T}.c)
    target_link_libraries (${T} glibc-compatibility)
    set_target_properties (${T} PROPERTIES LINKER_LANGUAGE CXX)
    add_check (${T})
endforeach ()
