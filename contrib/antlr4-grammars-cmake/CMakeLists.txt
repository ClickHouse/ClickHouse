set(ENABLE_ANTLR4_GRAMMARS_DEFAULT ${ENABLE_LIBRARIES})
option(ENABLE_ANTLR4_GRAMMARS "Use ANTLR4 grammars" ${ENABLE_ANTLR4_GRAMMARS_DEFAULT})

if(NOT ENABLE_ANTLR4_GRAMMARS)
  message(STATUS "Not using ANTLR4 grammars")
  return()
endif()

set(_ANTLR4_GRAMMARS_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/antlr4-grammars-cmake/generated")
set(_ANTLR4_GRAMMARS_SOURCE_DIR "${_ANTLR4_GRAMMARS_INCLUDE_DIR}/antlr4_grammars")

file(GLOB _ANTLR4_GRAMMARS_SRC "${_ANTLR4_GRAMMARS_SOURCE_DIR}/*.h" "${_ANTLR4_GRAMMARS_SOURCE_DIR}/*.cpp")

add_library(_ch_contrib_antlr4_grammars ${_ANTLR4_GRAMMARS_SRC})

target_include_directories(_ch_contrib_antlr4_grammars
    PUBLIC "${_ANTLR4_GRAMMARS_INCLUDE_DIR}"
)

target_link_libraries(_ch_contrib_antlr4_grammars PUBLIC ch_contrib::antlr4_cpp_runtime)

add_library(ch_contrib::antlr4_grammars ALIAS _ch_contrib_antlr4_grammars)
