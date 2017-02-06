if (USE_INTERNAL_POCO_LIBRARY)
	set (Poco_INCLUDE_DIRS
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Foundation/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Util/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Net/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Data/ODBC/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/Crypto/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/XML/include/"
		"${ClickHouse_SOURCE_DIR}/contrib/libpoco/MongoDB/include/"
	)

	if (USE_STATIC_LIBRARIES)
		set (Poco_INCLUDE_DIRS ${Poco_INCLUDE_DIRS} "${ClickHouse_SOURCE_DIR}/contrib/libzlib-ng/" "${ClickHouse_BINARY_DIR}/contrib/libzlib-ng/")
	endif ()

	set (Poco_Net_LIBRARY PocoNet)
	set (Poco_Util_LIBRARY PocoUtil)
	set (Poco_XML_LIBRARY PocoXML)
	set (Poco_Data_LIBRARY PocoData)
	set (Poco_Crypto_LIBRARY PocoCrypto)
	set (Poco_DataODBC_LIBRARY PocoDataODBC)
	set (Poco_MongoDB_LIBRARY PocoMongoDB)
	set (Poco_Foundation_LIBRARY PocoFoundation)
	include_directories (BEFORE ${Poco_INCLUDE_DIRS})
else ()
	find_package (Poco REQUIRED Util Net XML Data Crypto DataODBC MongoDB Foundation)
	include_directories (${Poco_INCLUDE_DIRS})
endif ()

message(STATUS "Using Poco: ${Poco_INCLUDE_DIRS} : ${Poco_Net_LIBRARY},${Poco_Util_LIBRARY},${Poco_XML_LIBRARY},${Poco_Data_LIBRARY},${Poco_DataODBC_LIBRARY},${Poco_MongoDB_LIBRARY},${Poco_Foundation_LIBRARY}")
