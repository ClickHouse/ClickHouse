option(ENABLE_CLIENT_AI "Enable AI SDK for SQL generation" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CLIENT_AI)
    message(STATUS "Not using ai-sdk-cpp")
    return()
endif()

set(AI_SDK_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/ai-sdk-cpp")
set(AI_SDK_THIRD_PARTY_DIR "${AI_SDK_SOURCE_DIR}/third_party")

# Core library sources
set(AI_SDK_CORE_SOURCES
    "${AI_SDK_SOURCE_DIR}/src/types/generate_options.cpp"
    "${AI_SDK_SOURCE_DIR}/src/types/stream_options.cpp"
    "${AI_SDK_SOURCE_DIR}/src/types/stream_result.cpp"
    "${AI_SDK_SOURCE_DIR}/src/types/message.cpp"
    "${AI_SDK_SOURCE_DIR}/src/http/http_request_handler.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/base_provider_client.cpp"
    "${AI_SDK_SOURCE_DIR}/src/tools/tool_executor.cpp"
    "${AI_SDK_SOURCE_DIR}/src/tools/multi_step_coordinator.cpp"
)

# OpenAI provider sources
set(AI_SDK_OPENAI_SOURCES
    "${AI_SDK_SOURCE_DIR}/src/providers/openai/openai_client.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/openai/openai_request_builder.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/openai/openai_response_parser.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/openai/openai_stream.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/openai/openai_factory.cpp"
)

# Anthropic provider sources
set(AI_SDK_ANTHROPIC_SOURCES
    "${AI_SDK_SOURCE_DIR}/src/providers/anthropic/anthropic_client.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/anthropic/anthropic_request_builder.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/anthropic/anthropic_response_parser.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/anthropic/anthropic_stream.cpp"
    "${AI_SDK_SOURCE_DIR}/src/providers/anthropic/anthropic_factory.cpp"
)

# Add nlohmann_json from submodule
set(NLOHMANN_JSON_SOURCE_DIR "${AI_SDK_THIRD_PARTY_DIR}/nlohmann_json_patched")
add_library(_ai_sdk_nlohmann_json INTERFACE)
target_include_directories(_ai_sdk_nlohmann_json SYSTEM INTERFACE
    "${NLOHMANN_JSON_SOURCE_DIR}/include"
)
add_library(ai_sdk_nlohmann_json::ai_sdk_nlohmann_json ALIAS _ai_sdk_nlohmann_json)

# Add httplib from submodule (header-only)
set(HTTPLIB_SOURCE_DIR "${AI_SDK_THIRD_PARTY_DIR}/httplib-header-only")
add_library(_httplib INTERFACE)
target_include_directories(_httplib SYSTEM INTERFACE
    "${HTTPLIB_SOURCE_DIR}"
)
target_link_libraries(_httplib INTERFACE
    OpenSSL::SSL
    OpenSSL::Crypto
    ch_contrib::brotli
)
add_library(httplib::httplib ALIAS _httplib)

# Add concurrentqueue from submodule (header-only)
set(CONCURRENTQUEUE_SOURCE_DIR "${AI_SDK_THIRD_PARTY_DIR}/concurrentqueue-header-only")
add_library(_concurrentqueue INTERFACE)
target_include_directories(_concurrentqueue SYSTEM INTERFACE
    "${CONCURRENTQUEUE_SOURCE_DIR}"
)
add_library(concurrentqueue ALIAS _concurrentqueue)

# Create core library
add_library(_ai-sdk-cpp-core ${AI_SDK_CORE_SOURCES})

target_include_directories(_ai-sdk-cpp-core SYSTEM
    PUBLIC
        "${AI_SDK_SOURCE_DIR}/include"
    PRIVATE
        "${AI_SDK_SOURCE_DIR}/src"
)

target_link_libraries(_ai-sdk-cpp-core
    PUBLIC
        ai_sdk_nlohmann_json::ai_sdk_nlohmann_json
    PRIVATE
        httplib::httplib
        concurrentqueue
)

target_compile_definitions(_ai-sdk-cpp-core
    PRIVATE
        CPPHTTPLIB_OPENSSL_SUPPORT=1
        CPPHTTPLIB_BROTLI_SUPPORT=1
        CPPHTTPLIB_THREAD_POOL_COUNT=8
)

# Create OpenAI provider library
add_library(_ai-sdk-cpp-openai ${AI_SDK_OPENAI_SOURCES})

target_include_directories(_ai-sdk-cpp-openai SYSTEM
    PUBLIC
        "${AI_SDK_SOURCE_DIR}/include"
    PRIVATE
        "${AI_SDK_SOURCE_DIR}/src"
)

target_link_libraries(_ai-sdk-cpp-openai
    PUBLIC
        _ai-sdk-cpp-core
        ai_sdk_nlohmann_json::ai_sdk_nlohmann_json
    PRIVATE
        httplib::httplib
        concurrentqueue
)

target_compile_definitions(_ai-sdk-cpp-openai
    PUBLIC
        AI_SDK_HAS_OPENAI=1
    PRIVATE
        CPPHTTPLIB_OPENSSL_SUPPORT=1
        CPPHTTPLIB_BROTLI_SUPPORT=1
        CPPHTTPLIB_THREAD_POOL_COUNT=8
)

# Create Anthropic provider library
add_library(_ai-sdk-cpp-anthropic ${AI_SDK_ANTHROPIC_SOURCES})

target_include_directories(_ai-sdk-cpp-anthropic SYSTEM
    PUBLIC
        "${AI_SDK_SOURCE_DIR}/include"
    PRIVATE
        "${AI_SDK_SOURCE_DIR}/src"
)

target_link_libraries(_ai-sdk-cpp-anthropic
    PUBLIC
        _ai-sdk-cpp-core
        ai_sdk_nlohmann_json::ai_sdk_nlohmann_json
    PRIVATE
        httplib::httplib
        concurrentqueue
)

target_compile_definitions(_ai-sdk-cpp-anthropic
    PUBLIC
        AI_SDK_HAS_ANTHROPIC=1
    PRIVATE
        CPPHTTPLIB_OPENSSL_SUPPORT=1
        CPPHTTPLIB_BROTLI_SUPPORT=1
        CPPHTTPLIB_THREAD_POOL_COUNT=8
)

# Create main interface library
add_library(_ai-sdk-cpp INTERFACE)

target_include_directories(_ai-sdk-cpp SYSTEM INTERFACE
    "${AI_SDK_SOURCE_DIR}/include"
)

target_link_libraries(_ai-sdk-cpp
    INTERFACE
        _ai-sdk-cpp-core
        _ai-sdk-cpp-openai
        _ai-sdk-cpp-anthropic
)

target_compile_definitions(_ai-sdk-cpp
    INTERFACE
        AI_SDK_HAS_OPENAI=1
        AI_SDK_HAS_ANTHROPIC=1
)

# Create main alias for ClickHouse usage
add_library(ch_contrib::ai-sdk-cpp ALIAS _ai-sdk-cpp)

# Create ai:: namespace alias for compatibility with SDK examples
add_library(ai::sdk ALIAS _ai-sdk-cpp)

# Apply C++20 standard
set_target_properties(_ai-sdk-cpp-core _ai-sdk-cpp-openai _ai-sdk-cpp-anthropic
    PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF
)

# Third party library may have substandard code
target_compile_options(_ai-sdk-cpp-core PRIVATE -w)
target_compile_options(_ai-sdk-cpp-openai PRIVATE -w)
target_compile_options(_ai-sdk-cpp-anthropic PRIVATE -w)

message(STATUS "Using ai-sdk-cpp with OpenAI and Anthropic support")
