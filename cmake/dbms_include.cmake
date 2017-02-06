
include_directories (${CMAKE_SOURCE_DIR}/dbms/include)

# TODO:
# move code with incldes from .h to .cpp and clean this list:
include_directories (${CMAKE_SOURCE_DIR}/libs/libcommon/include)
# for generated config_version.h and config_common.h:
include_directories (${CMAKE_BINARY_DIR}/libs/libcommon/include)
include_directories (${CMAKE_SOURCE_DIR}/libs/libpocoext/include)
include_directories (${CMAKE_SOURCE_DIR}/libs/libzkutil/include)
include_directories (${CMAKE_SOURCE_DIR}/libs/libmysqlxx/include)
include_directories (BEFORE ${CMAKE_SOURCE_DIR}/contrib/libzookeeper/include)
include_directories (BEFORE ${CMAKE_SOURCE_DIR}/contrib/libcityhash/include)
include_directories (BEFORE ${CMAKE_SOURCE_DIR}/contrib/libdouble-conversion)
