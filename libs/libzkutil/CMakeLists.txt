include_directories (include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_include.cmake)

add_library(zkutil
	src/ZooKeeper.cpp
	src/Lock.cpp
	src/SingleBarrier.cpp
	src/RWLock.cpp
	src/ZooKeeperHolder.cpp

	include/zkutil/Increment.h
    include/zkutil/LeaderElection.h
    include/zkutil/KeeperException.h
    include/zkutil/Common.h
    include/zkutil/Lock.h
    include/zkutil/SingleBarrier.h
    include/zkutil/RWLock.h
    include/zkutil/ZooKeeper.h
    include/zkutil/Types.h
    include/zkutil/ZooKeeperHolder.h)

find_package (Threads)

target_link_libraries(zkutil ${ZOOKEEPER_LIBRARY} ${Poco_Foundation_LIBRARY} ${CMAKE_THREAD_LIBS_INIT} string_utils)

add_subdirectory (src)
