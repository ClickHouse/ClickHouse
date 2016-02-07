add_library (pocoext
	src/LevelFilterChannel.cpp
	src/ThreadNumber.cpp

	include/Poco/Ext/LevelFilterChannel.h
	include/Poco/Ext/ThreadNumber.h)

add_dependencies (pocoext PocoUtil PocoNet PocoXML PocoFoundation)
target_link_libraries(pocoext PocoUtil PocoNet PocoXML PocoFoundation)

add_subdirectory (utils)
add_executable (make-charset utils/make-charset.cpp)

ADD_SUBDIRECTORY (charsets/src)
ADD_SUBDIRECTORY (charsets/include/Poco)

# build charsets

set(SRCS )

macro (build_charset txtfile)
GET_FILENAME_COMPONENT(dstname ${txtfile} NAME_WE)
add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/charsets/src/${dstname}.cpp" "${CMAKE_CURRENT_BINARY_DIR}/charsets/include/Poco/${dstname}.h"
    COMMAND cat ${txtfile} | $<TARGET_FILE:make-charset> ${dstname} "${CMAKE_CURRENT_BINARY_DIR}/charsets/src/${dstname}.cpp" "${CMAKE_CURRENT_BINARY_DIR}/charsets/include/Poco/${dstname}.h"
    COMMENT "Building charset ${txtfile} with make-charset"
    DEPENDS ${txtfile} make-charset
)
SET(SRCS ${SRCS} "${CMAKE_CURRENT_BINARY_DIR}/charsets/src/${dstname}.cpp")
endmacro (build_charset)

file(GLOB charset_files "${CMAKE_CURRENT_SOURCE_DIR}/charsets/txts/*.txt")
foreach(__item_ ${charset_files})
    build_charset(${__item_})
endforeach(__item_)

ADD_LIBRARY(charsets ${SRCS})
