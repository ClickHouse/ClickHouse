set(ENABLE_ANTLR4_CPP_RUNTIME_DEFAULT ${ENABLE_LIBRARIES})
option(ENABLE_ANTLR4_CPP_RUNTIME "Use ANTLR4 C++ runtime" ${ENABLE_ANTLR4_CPP_RUNTIME_DEFAULT})

if(NOT ENABLE_ANTLR4_CPP_RUNTIME)
  message(STATUS "Not using ANTLR4 C++ runtime")
  return()
endif()

set(_ANTLR4_CPP_RUNTIME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/antlr4-cpp-runtime/runtime/Cpp")

file(GLOB _ANTLR4_CPP_RUNTIME_SRC
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/atn/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/dfa/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/internal/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/misc/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/support/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree/pattern/*.cpp"
  "${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree/xpath/*.cpp"
)

set(_ANTLR4_CPP_RUNTIME_INCLUDE_DIRS
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src
)

set(_ANTLR4_CPP_RUNTIME_PRIVATE_INCLUDE_DIRS
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/atn
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/dfa
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/internal
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/misc
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/support
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree/pattern
  ${_ANTLR4_CPP_RUNTIME_SOURCE_DIR}/runtime/src/tree/xpath
)

add_library(_ch_contrib_antlr4_cpp_runtime ${_ANTLR4_CPP_RUNTIME_SRC})

target_include_directories(_ch_contrib_antlr4_cpp_runtime
    PUBLIC "${_ANTLR4_CPP_RUNTIME_INCLUDE_DIRS}"
    PRIVATE "${_ANTLR4_CPP_RUNTIME_PRIVATE_INCLUDE_DIRS}"
)

add_library(ch_contrib::antlr4_cpp_runtime ALIAS _ch_contrib_antlr4_cpp_runtime)
