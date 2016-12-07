include_directories (include)
include_directories (/usr/include/mysql)

add_subdirectory (src)


if (ENABLE_MONGODB)
	set (LINK_MONGOCLIENT libmongoclient.a ${OPENSSL_LIBRARIES} ${Boost_THREAD_LIBRARY})
	set (MONGODB_FILES
		include/DB/Dictionaries/MongoDBBlockInputStream.h
		include/DB/Dictionaries/MongoDBDictionarySource.h
		src/Dictionaries/MongoDBDictionarySource.cpp
		src/Dictionaries/MongoDBBlockInputStream.cpp)
	add_definitions(-D ENABLE_MONGODB)
endif ()


if (NOT ENABLE_LIBTCMALLOC)
	add_definitions(-D NO_TCMALLOC)
endif ()


add_library(string_utils
	include/DB/Common/StringUtils.h
	src/Common/StringUtils.cpp)


set(dbms_headers)
set(dbms_sources)

include(../cmake/dbms_glob_sources.cmake)
add_headers_and_sources(dbms Functions)
add_headers_and_sources(dbms Functions/Conditional)
add_headers_and_sources(dbms TableFunctions)
add_headers_and_sources(dbms Parsers)
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
add_headers_and_sources(dbms Storages/System)
add_headers_and_sources(dbms Core)
add_headers_and_sources(dbms Client)
add_headers_only(dbms Server)

list(REMOVE_ITEM dbms_sources
	src/Client/Client.cpp
	src/Client/Benchmark.cpp
	src/Storages/StorageCloud.cpp
	src/Databases/DatabaseCloud.cpp
	src/Common/StringUtils.cpp)

if (APPLE)
	list(REMOVE_ITEM dbms_headers
		include/DB/Common/AIO.h
		include/DB/IO/WriteBufferAIO.h
		include/DB/IO/ReadBufferAIO.h)
	list(REMOVE_ITEM dbms_sources
		src/IO/ReadBufferAIO.cpp
		src/IO/WriteBufferAIO.cpp)
endif()

add_library(dbms ${dbms_headers} ${dbms_sources})


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

set (APPLE_ICONV_LIB "")
if (APPLE)
	set(APPLE_ICONV_LIB iconv)
endif()

target_link_libraries(dbms
	common
	zkutil
	mysqlxx
	cityhash farmhash metrohash
	lz4 zstd
	string_utils
	double-conversion
	${LINK_LIBRARIES_ONLY_ON_X86_64}
	re2 re2_st
	${OPENSSL_CRYPTO_LIBRARY}
	${Boost_SYSTEM_LIBRARY}
	${LINK_MONGOCLIENT}
	${Boost_REGEX_LIBRARY}
	PocoData
	PocoDataODBC
	${APPLE_ICONV_LIB}
	dl ${LTDL_LIB})

add_dependencies (dbms
	common
	zkutil)
