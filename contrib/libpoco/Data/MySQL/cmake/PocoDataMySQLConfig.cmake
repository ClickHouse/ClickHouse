include(CMakeFindDependencyMacro)
find_dependency(PocoFoundation)
find_dependency(PocoData)
include("${CMAKE_CURRENT_LIST_DIR}/PocoDataMySQLTargets.cmake")
