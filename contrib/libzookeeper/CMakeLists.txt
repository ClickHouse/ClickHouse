add_definitions(-DHAVE_CONFIG_H -DTHREADED)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
	add_definitions(-Wno-unused-but-set-variable)
endif()

add_library (zookeeper
src/zookeeper.c
src/zookeeper.jute.c
src/zk_hashtable.c
src/zk_log.c
src/mt_adaptor.c
src/recordio.c
src/hashtable/hashtable.c
src/hashtable/hashtable_itr.c
)

target_include_directories (zookeeper
	PRIVATE include/zookeeper
	PRIVATE src
	INTERFACE include
)