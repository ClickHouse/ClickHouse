add_library (pocoext
	src/LevelFilterChannel.cpp
	src/ThreadNumber.cpp

	include/Poco/Ext/LevelFilterChannel.h
	include/Poco/Ext/ThreadNumber.h)

add_dependencies (pocoext PocoUtil PocoNet PocoXML PocoFoundation)
target_link_libraries(pocoext PocoUtil PocoNet PocoXML PocoFoundation)
