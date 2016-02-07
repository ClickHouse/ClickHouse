add_library (daemon
	src/Daemon.cpp
	src/GraphiteWriter.cpp

	include/daemon/Daemon.h
	include/daemon/GraphiteWriter.h
)

target_link_libraries (daemon dbms)
