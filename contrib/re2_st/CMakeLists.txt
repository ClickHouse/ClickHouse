# Building re2 which is thread-safe and re2_st which is not.
# re2 changes its state during matching of regular expression, e.g. creates temporary DFA.
# It uses RWLock to process the same regular expression object from different threads.
# In order to avoid redundant locks in some cases, we use not thread-safe version of the library (re2_st).

set (RE2_SOURCE_DIR ${ClickHouse_SOURCE_DIR}/contrib/re2/)

get_target_property (RE2_SOURCES_ re2 SOURCES)
foreach (src ${RE2_SOURCES_})
    list(APPEND RE2_ST_SOURCES ${RE2_SOURCE_DIR}/${src})
endforeach ()

add_library (re2_st ${RE2_ST_SOURCES})
target_compile_definitions (re2_st PRIVATE NDEBUG NO_THREADS re2=re2_st)
target_include_directories (re2_st PRIVATE . PUBLIC ${CMAKE_CURRENT_BINARY_DIR} ${RE2_SOURCE_DIR})

file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/re2_st)
foreach (FILENAME filtered_re2.h re2.h set.h stringpiece.h)
    add_custom_command (OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}"
        COMMAND ${CMAKE_COMMAND} -DSOURCE_FILENAME="${RE2_SOURCE_DIR}/re2/${FILENAME}"
            -DTARGET_FILENAME="${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/re2_transform.cmake"
        COMMENT "Creating ${FILENAME} for re2_st library.")
    add_custom_target (transform_${FILENAME} DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/re2_st/${FILENAME}")
    add_dependencies (re2_st transform_${FILENAME})
endforeach ()

file (MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/util)
foreach (FILENAME mutex.h)
    add_custom_command (OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}"
        COMMAND ${CMAKE_COMMAND} -DSOURCE_FILENAME="${RE2_SOURCE_DIR}/util/${FILENAME}"
            -DTARGET_FILENAME="${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/re2_transform.cmake"
        COMMENT "Creating ${FILENAME} for re2_st library.")
    add_custom_target (transform_${FILENAME} DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/util/${FILENAME}")
    add_dependencies (re2_st transform_${FILENAME})
endforeach ()
