# Poco::XML (expat)

file (GLOB SRCS_EXPAT src/xml*.c)

add_library (_poco_xml_expat ${SRCS_EXPAT})
add_library (Poco::XML::Expat ALIAS _poco_xml_expat)

# TODO: remove these warning exclusions
target_compile_options (_poco_xml_expat
    PRIVATE
        -Wno-cast-qual
        -Wno-empty-translation-unit
        -Wno-extra-semi-stmt
        -Wno-implicit-fallthrough
        -Wno-reserved-identifier
        -Wno-unused-macros
)

target_include_directories (_poco_xml_expat PUBLIC "include")
target_include_directories (_poco_xml_expat PRIVATE "../Foundation/include")

# Poco::XML

file (GLOB SRCS src/*.cpp)
add_library (_poco_xml ${SRCS})
add_library (Poco::XML ALIAS _poco_xml)
add_compile_definitions (_poco_xml PRIVATE XML_GE=0)

# TODO: remove these warning exclusions
target_compile_options (_poco_xml
    PRIVATE
        -Wno-cast-qual
        -Wno-covered-switch-default
        -Wno-deprecated-dynamic-exception-spec
        -Wno-implicit-fallthrough
        -Wno-missing-noreturn
        -Wno-old-style-cast
        -Wno-reserved-identifier
        -Wno-shadow
        -Wno-shorten-64-to-32
        -Wno-suggest-destructor-override
        -Wno-suggest-override
        -Wno-tautological-type-limit-compare
        -Wno-tautological-unsigned-zero-compare
        -Wno-unreachable-code
        -Wno-unreachable-code-break
        -Wno-unused-macros
        -Wno-unused-parameter
        -Wno-zero-as-null-pointer-constant
)
target_include_directories (_poco_xml SYSTEM PUBLIC "include")
target_link_libraries (_poco_xml PUBLIC Poco::Foundation Poco::XML::Expat)
