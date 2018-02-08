if (ARCH_ARM)
    # bundled have some asm broken for arm, use package libzookeeper-mt-dev
    set(USE_INTERNAL_ZOOKEEPER_LIBRARY 0 CACHE BOOL "")
endif ()

option (USE_INTERNAL_ZOOKEEPER_LIBRARY "Set to FALSE to use system zookeeper library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_ZOOKEEPER_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/zookeeper/src/c/CMakeLists.txt")
   message (WARNING "submodule contrib/zookeeper is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_ZOOKEEPER_LIBRARY 0)
endif ()

if (NOT USE_INTERNAL_ZOOKEEPER_LIBRARY)
    find_library (ZOOKEEPER_LIBRARY zookeeper_mt)
    find_path (ZOOKEEPER_INCLUDE_DIR NAMES zookeeper/zookeeper.h PATHS ${ZOOKEEPER_INCLUDE_PATHS})
    set(ZOOKEEPER_INCLUDE_DIR "${ZOOKEEPER_INCLUDE_DIR}/zookeeper")
endif ()

if (ZOOKEEPER_LIBRARY AND ZOOKEEPER_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_ZOOKEEPER_LIBRARY 1)
    set(WANT_CPPUNIT 0 CACHE BOOL "")
    set (ZOOKEEPER_LIBRARY zookeeper)
endif ()

message (STATUS "Using zookeeper: ${ZOOKEEPER_INCLUDE_DIR} : ${ZOOKEEPER_LIBRARY}")


# how to make clickhouse branch of https://github.com/ClickHouse-Extras/zookeeper.git :
# git remote add upstream https://github.com/apache/zookeeper.git
# git checkhout upstream/master
# git branch -D clickhouse
# git checkout -b clickhouse
# git merge clickhouse_misc
# git merge clickhouse_706
