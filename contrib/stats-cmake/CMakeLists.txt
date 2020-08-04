# The stats is a header-only library of probability density functions,
# cumulative distribution functions, quantile functions, and random sampling methods.
set(STATS_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/stats/include)
set(GCEM_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/gcem/include)
set(EIGEN_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/eigen)

add_library(stats INTERFACE)

target_include_directories(stats SYSTEM INTERFACE ${STATS_INCLUDE_DIR})
target_include_directories(stats SYSTEM INTERFACE ${GCEM_INCLUDE_DIR})
target_include_directories(stats SYSTEM INTERFACE ${EIGEN_INCLUDE_DIR})
