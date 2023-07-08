# Adding test output on failure
enable_testing ()

if (NOT TARGET check)
    if (CMAKE_CONFIGURATION_TYPES)
        add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND}
            --force-new-ctest-process --output-on-failure --build-config "$<CONFIGURATION>"
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    else ()
        add_custom_target (check COMMAND ${CMAKE_CTEST_COMMAND}
            --force-new-ctest-process --output-on-failure
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
    endif ()
endif ()

macro (add_check target)
    add_test (NAME test_${target} COMMAND ${target} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    add_dependencies (check ${target})
endmacro (add_check)
