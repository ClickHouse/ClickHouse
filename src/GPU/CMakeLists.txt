# The GPU engine - the aggregation and the hash join - in two targets, one per toolchain, meeting
# at the C boundary declared in GPUAggregationABI.h.
#
#   clickhouse_gpu_cudf  nvcc's gcc host pass, the system libstdc++, cuDF's headers
#   clickhouse_gpu       clang, ClickHouse's own libc++, warning set and include tree
#
# Two of them because CMake has one C++ compiler per project and ClickHouse's global compile
# options do not survive nvcc - cmake/cuda.cmake explains that at length. The split also keeps
# cuDF's headers, and the standard library they are compiled against, out of the translation units
# of a server that merely wants to sum a column.
#
# The join's ClickHouse half is not here: it implements `IJoin` and lives with the other join
# algorithms, in src/Interpreters/GPUHashJoin.cpp, from where it reaches the boundary header
# directly and the host-side type mapping through GPUAggregation.h.

add_library (clickhouse_gpu_cudf STATIC GPUAggregationCudf.cpp)

# Drops what the target inherited from ClickHouse's toolchain and compiles its .cpp files as CUDA.
# Has to come before the target_* calls below, or it clears those as well.
gpu_island_target (clickhouse_gpu_cudf)

# The one ClickHouse include path this half gets: enough to reach the boundary header under the
# same name from both sides, and not enough to reach a ClickHouse header that would not compile
# here. Everything else it includes comes from cuDF.
target_include_directories (clickhouse_gpu_cudf SYSTEM PRIVATE "${ClickHouse_SOURCE_DIR}/src")

# PUBLIC, so that the executable which ends up carrying this archive also gets cuDF's and the
# static CUDA runtime. The libstdc++ this half is compiled against is deliberately not named here:
# it has to be the last library on the link line, so it comes from
# cmake/linux/default_libs.cmake instead.
target_link_libraries (clickhouse_gpu_cudf PUBLIC
    ch_contrib::cudf
    ch_gpu::cudart
)

add_library (clickhouse_gpu STATIC GPUAggregation.cpp GPUColumnCache.cpp)

target_link_libraries (clickhouse_gpu PUBLIC clickhouse_gpu_cudf)

# For the ClickHouse headers this half uses. The symbols behind them live in `dbms`, which links
# this target, and are resolved when an executable is linked.
target_link_libraries (clickhouse_gpu PRIVATE clickhouse_common_io)
