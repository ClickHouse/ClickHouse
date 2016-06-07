include(CMakeFindDependencyMacro)
find_dependency(PocoFoundation)
find_dependency(PocoNet)
include("${CMAKE_CURRENT_LIST_DIR}/PocoMongoDBTargets.cmake")
