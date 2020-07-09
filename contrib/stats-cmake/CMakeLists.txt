# The stats is a header-only library of probability density functions, 
# cumulative distribution functions, quantile functions, and random sampling methods.

set(STATS_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/stats/include)
set(GCEM_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/gcem/include)

add_library(stats)

target_include_directories(stats SYSTEM PUBLIC ${STATS_INCLUDE_DIR})
target_include_directories(stats SYSTEM PUBLIC ${GCEM_INCLUDE_DIR})

add_dependencies(stats gcem)
