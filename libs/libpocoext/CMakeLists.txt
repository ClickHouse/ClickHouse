add_library (pocoext
	src/LevelFilterChannel.cpp
	src/ThreadNumber.cpp

	include/Poco/Ext/LevelFilterChannel.h
	include/Poco/Ext/ThreadNumber.h)

if (USE_INTERNAL_POCO_LIBRARY)
	add_dependencies (pocoext ${Poco_Util_LIBRARY} ${Poco_Net_LIBRARY} ${Poco_XML_LIBRARY} ${Poco_Foundation_LIBRARY})
endif ()
target_link_libraries(pocoext ${Poco_Util_LIBRARY} ${Poco_Net_LIBRARY} ${Poco_XML_LIBRARY} ${Poco_Foundation_LIBRARY})
