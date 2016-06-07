add_library (daemon
	src/BaseDaemon.cpp
	src/GraphiteWriter.cpp

	include/daemon/BaseDaemon.h
	include/daemon/GraphiteWriter.h
)

target_link_libraries (daemon dbms)
