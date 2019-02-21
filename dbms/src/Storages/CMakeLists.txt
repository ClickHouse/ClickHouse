add_subdirectory(System)
add_subdirectory(Kafka)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
