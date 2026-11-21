file (GLOB wyrd_sources CONFIGURE_DEPENDS *.cpp Facade/*.cpp Testing/*.cpp)

add_library (clickhouse_wyrd ${wyrd_sources})
target_link_libraries (clickhouse_wyrd PUBLIC clickhouse_common_io)

if (ENABLE_TESTS)
    foreach (strategy Adversary Simulation)
        string (TOLOWER ${strategy} suffix)
        string (TOUPPER ${strategy} define)
        file (GLOB strategy_sources CONFIGURE_DEPENDS Strategies/${strategy}/*.cpp)

        add_library (clickhouse_wyrd_${suffix} ${wyrd_sources} ${strategy_sources})
        target_compile_definitions (clickhouse_wyrd_${suffix} PUBLIC WYRD_${define}=1)
        target_compile_options (clickhouse_wyrd_${suffix} PUBLIC $<TARGET_PROPERTY:unit_tests_dbms,COMPILE_OPTIONS>)
        target_link_libraries (clickhouse_wyrd_${suffix} PUBLIC clickhouse_common_io ch_contrib::gtest)
    endforeach ()

    clickhouse_add_wyrd_tests (tests/*.cpp)
endif ()
