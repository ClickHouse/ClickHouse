This directory contains the unmodified library sources from `libmaxminddb`
1.14.1, commit `4a3d17a621c0ebcb81498fd34d313f67fd940ab5`:
https://github.com/maxmind/libmaxminddb/tree/4a3d17a621c0ebcb81498fd34d313f67fd940ab5

Only the library sources, public headers, CMake configuration-header template,
and Apache-2.0 license are included. Upstream test dependencies use recursive
submodules, which ClickHouse does not permit. ClickHouse builds these sources
through `contrib/libmaxminddb-cmake/CMakeLists.txt`.
