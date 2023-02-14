if (TARGET ch_rust::prql_lib)
    list (APPEND PUBLIC_LIBS
        ch_rust::prql_lib
    )
endif()

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

# target_link_libraries(dbms PUBLIC ${PUBLIC_LIBS} PRIVATE ${PRIVATE_LIBS})
