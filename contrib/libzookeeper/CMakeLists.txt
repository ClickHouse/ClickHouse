
add_library (zookeeper_mt
src/zookeeper.c
src/zookeeper.jute.c
src/zk_hashtable.c
src/zk_log.c
src/mt_adaptor.c
src/recordio.c
src/hashtable/hashtable.c
src/hashtable/hashtable_itr.c
)

target_compile_definitions (zookeeper_mt PRIVATE HAVE_CONFIG_H THREADED)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options (zookeeper_mt PRIVATE -Wno-unused-but-set-variable)
endif()

target_include_directories (zookeeper_mt
    PRIVATE include/zookeeper
    PRIVATE src
    INTERFACE include
)
