# Poco::XML (expat)

file (GLOB SRCS_EXPAT src/xml*.c)

add_library (_poco_xml_expat ${SRCS_EXPAT})
add_library (Poco::XML::Expat ALIAS _poco_xml_expat)

target_include_directories (_poco_xml_expat PUBLIC "include")
target_include_directories (_poco_xml_expat PRIVATE "../Foundation/include")

# Poco::XML

file (GLOB SRCS src/*.cpp)
add_library (_poco_xml ${SRCS})
add_library (Poco::XML ALIAS _poco_xml)

target_compile_options (_poco_xml PRIVATE -Wno-old-style-cast)
target_include_directories (_poco_xml SYSTEM PUBLIC "include")
target_link_libraries (_poco_xml PUBLIC Poco::Foundation Poco::XML::Expat)
