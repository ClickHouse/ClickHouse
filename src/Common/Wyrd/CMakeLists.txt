file (GLOB wyrd_sources CONFIGURE_DEPENDS *.cpp Facade/*.cpp Testing/*.cpp)

add_library (clickhouse_wyrd ${wyrd_sources})
target_link_libraries (clickhouse_wyrd PUBLIC clickhouse_common_io)

if (ENABLE_TESTS)
    file (GLOB strategy_sources CONFIGURE_DEPENDS Strategies/*.cpp Strategies/*/*.cpp)

    add_library (clickhouse_wyrd_enabled ${wyrd_sources} ${strategy_sources})
    target_compile_definitions (clickhouse_wyrd_enabled PUBLIC WYRD_ENABLED=1)
    target_compile_options (clickhouse_wyrd_enabled PUBLIC $<TARGET_PROPERTY:unit_tests_dbms,COMPILE_OPTIONS>)
    target_link_libraries (clickhouse_wyrd_enabled PUBLIC clickhouse_common_io ch_contrib::gtest)

    clickhouse_add_wyrd_tests (tests/*.cpp)
endif ()
