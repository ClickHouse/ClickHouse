set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/yaml-cpp)

set (SRCS
	${LIBRARY_DIR}/src/binary.cpp
	${LIBRARY_DIR}/src/emitterutils.cpp
	${LIBRARY_DIR}/src/null.cpp
	${LIBRARY_DIR}/src/scantoken.cpp
	${LIBRARY_DIR}/src/convert.cpp
	${LIBRARY_DIR}/src/exceptions.cpp
	${LIBRARY_DIR}/src/ostream_wrapper.cpp
	${LIBRARY_DIR}/src/simplekey.cpp
	${LIBRARY_DIR}/src/depthguard.cpp 
	${LIBRARY_DIR}/src/exp.cpp
	${LIBRARY_DIR}/src/parse.cpp
	${LIBRARY_DIR}/src/singledocparser.cpp
	${LIBRARY_DIR}/src/directives.cpp
	${LIBRARY_DIR}/src/memory.cpp
	${LIBRARY_DIR}/src/parser.cpp
	${LIBRARY_DIR}/src/stream.cpp
	${LIBRARY_DIR}/src/emit.cpp
	${LIBRARY_DIR}/src/nodebuilder.cpp
	${LIBRARY_DIR}/src/regex_yaml.cpp
	${LIBRARY_DIR}/src/tag.cpp
	${LIBRARY_DIR}/src/emitfromevents.cpp
	${LIBRARY_DIR}/src/node.cpp
	${LIBRARY_DIR}/src/scanner.cpp
	${LIBRARY_DIR}/src/emitter.cpp
	${LIBRARY_DIR}/src/node_data.cpp
	${LIBRARY_DIR}/src/scanscalar.cpp
	${LIBRARY_DIR}/src/emitterstate.cpp
	${LIBRARY_DIR}/src/nodeevents.cpp
	${LIBRARY_DIR}/src/scantag.cpp
)

add_library (yaml-cpp ${SRCS})


target_include_directories(yaml-cpp PRIVATE ${LIBRARY_DIR}/include/yaml-cpp)
target_include_directories(yaml-cpp SYSTEM BEFORE PUBLIC ${LIBRARY_DIR}/include)
