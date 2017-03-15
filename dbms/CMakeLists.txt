include(${ClickHouse_SOURCE_DIR}/cmake/dbms_include.cmake)

include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libdivide)
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libcpuid/include)
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libfarmhash)
include_directories (BEFORE ${ClickHouse_SOURCE_DIR}/contrib/libmetrohash/src)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)
include_directories (${ODBC_INCLUDE_DIRECTORIES})
include_directories (${ClickHouse_BINARY_DIR}/dbms/include)

if (NOT NO_WERROR)
	set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
endif ()

find_package (Threads)

add_subdirectory (src)

add_library(string_utils
	include/DB/Common/StringUtils.h
	src/Common/StringUtils.cpp)

set(dbms_headers)
set(dbms_sources)

include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(dbms Functions)
add_headers_and_sources(dbms Functions/Conditional)
add_headers_and_sources(dbms TableFunctions)
add_headers_and_sources(dbms Parsers)
add_headers_and_sources(dbms Analyzers)
add_headers_and_sources(dbms AggregateFunctions)
add_headers_and_sources(dbms Core)
add_headers_and_sources(dbms DataStreams)
add_headers_and_sources(dbms DataTypes)
add_headers_and_sources(dbms Databases)
add_headers_and_sources(dbms DataBases/Distributed)
add_headers_and_sources(dbms Dictionaries)
add_headers_and_sources(dbms Dictionaries/Embedded)
add_headers_and_sources(dbms Interpreters)
add_headers_and_sources(dbms Interpreters/ClusterProxy)
add_headers_and_sources(dbms Common)
add_headers_and_sources(dbms Common/HashTable)
add_headers_and_sources(dbms IO)
add_headers_and_sources(dbms Columns)
add_headers_and_sources(dbms Storages)
add_headers_and_sources(dbms Storages/Distributed)
add_headers_and_sources(dbms Storages/MergeTree)
add_headers_and_sources(dbms Core)
add_headers_and_sources(dbms Client)
add_headers_only(dbms Server)

list(REMOVE_ITEM dbms_sources
	src/Client/Client.cpp
	src/Client/Benchmark.cpp
	src/Storages/StorageCloud.cpp
	src/Databases/DatabaseCloud.cpp
	src/Common/StringUtils.cpp)

if (APPLE OR CMAKE_SYSTEM MATCHES "FreeBSD")
	list(REMOVE_ITEM dbms_headers
		include/DB/Common/AIO.h
		include/DB/IO/WriteBufferAIO.h
		include/DB/IO/ReadBufferAIO.h)
	list(REMOVE_ITEM dbms_sources
		src/IO/ReadBufferAIO.cpp
		src/IO/WriteBufferAIO.cpp)
endif()

include (${CMAKE_SOURCE_DIR}/libs/libcommon/cmake/version.cmake)

if (USE_STATIC_LIBRARIES)
	add_library(dbms ${dbms_headers} ${dbms_sources})
else ()
	add_library(dbms SHARED ${dbms_headers} ${dbms_sources})
	set_target_properties (dbms PROPERTIES SOVERSION ${VERSION_MAJOR} VERSION ${VERSION_SO} OUTPUT_NAME clickhouse)
	install (TARGETS dbms LIBRARY DESTINATION lib COMPONENT clickhouse)
endif ()


if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
	# Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
	set_source_files_properties(
		src/Functions/FunctionsArithmetic.cpp
		src/Functions/FunctionsArray.cpp
		src/Functions/FunctionsCoding.cpp
		src/Functions/FunctionsComparison.cpp
		src/Functions/FunctionsConditional.cpp
		src/Functions/FunctionsConversion.cpp
		src/Functions/FunctionsDateTime.cpp
		src/Functions/FunctionsDictionaries.cpp
		src/Functions/FunctionsFormatting.cpp
		src/Functions/FunctionsHashing.cpp
		src/Functions/FunctionsHigherOrder.cpp
		src/Functions/FunctionsLogical.cpp
		src/Functions/FunctionsRandom.cpp
		src/Functions/FunctionsReinterpret.cpp
		src/Functions/FunctionsRound.cpp
		src/Functions/FunctionsString.cpp
		src/Functions/FunctionsStringArray.cpp
		src/Functions/FunctionsStringSearch.cpp
		src/Functions/FunctionsURL.cpp
		src/Functions/FunctionsVisitParam.cpp
		src/Functions/FunctionsMath.cpp
		src/Functions/FunctionsGeo.cpp
		src/Functions/FunctionsMiscellaneous.cpp
		src/Functions/FunctionsTransform.cpp
		src/Dictionaries/FlatDictionary.cpp
		src/Dictionaries/HashedDictionary.cpp
		src/Dictionaries/CacheDictionary.cpp
		src/Dictionaries/RangeHashedDictionary.cpp
		src/Dictionaries/ComplexKeyHashedDictionary.cpp
		src/Dictionaries/ComplexKeyCacheDictionary.cpp
		PROPERTIES COMPILE_FLAGS -g0)
endif ()

if (NOT AARCH64)
	set (LINK_LIBRARIES_ONLY_ON_X86_64 cpuid)
endif()

if (CMAKE_SYSTEM MATCHES "FreeBSD")
    set(PLATFORM_LIBS "execinfo")
else()
    set(PLATFORM_LIBS "")
endif()

target_link_libraries(dbms
	common
	zkutil
	mysqlxx
	cityhash farmhash metrohash
	${LZ4_LIBRARY}
	${ZSTD_LIBRARY}
	string_utils
	${DOUBLE_CONVERSION_LIBRARY}
	${ZLIB_LIBRARIES}
	${LINK_LIBRARIES_ONLY_ON_X86_64}
	${RE2_LIBRARY}
	${RE2_ST_LIBRARY}
	${OPENSSL_CRYPTO_LIBRARY}
	${Boost_SYSTEM_LIBRARY}
	${Poco_Data_LIBRARY}
	${Poco_DataODBC_LIBRARY}
	${Poco_MongoDB_LIBRARY}
	${Poco_Foundation_LIBRARY}
	${Poco_NetSSL_LIBRARY}
	${PLATFORM_LIBS}
	${CMAKE_DL_LIBS}
	${LTDL_LIB}
	${CMAKE_THREAD_LIBS_INIT}
	)
