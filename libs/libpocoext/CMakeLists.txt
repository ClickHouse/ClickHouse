include_directories (include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include)

add_library (pocoext
    src/LevelFilterChannel.cpp
    src/ThreadNumber.cpp

    include/Poco/Ext/LevelFilterChannel.h
    include/Poco/Ext/ThreadNumber.h)

target_link_libraries(pocoext ${Poco_Util_LIBRARY} ${Poco_Net_LIBRARY} ${Poco_XML_LIBRARY} ${Poco_Foundation_LIBRARY})
