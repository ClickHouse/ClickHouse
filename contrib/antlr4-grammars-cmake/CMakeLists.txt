set(ENABLE_ANTLR4_GRAMMARS_DEFAULT ${ENABLE_LIBRARIES})
option(ENABLE_ANTLR4_GRAMMARS "Use ANTLR4 grammars" ${ENABLE_ANTLR4_GRAMMARS_DEFAULT})

if(NOT ENABLE_ANTLR4_GRAMMARS)
  message(STATUS "Not using ANTLR4 grammars")
  return()
endif()

set(_ANTLR4_GRAMMARS_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/antlr4-grammars-cmake/generated")
set(_ANTLR4_GRAMMARS_INCLUDE_DIR "${_ANTLR4_GRAMMARS_SOURCE_DIR}")

set(_ANTLR4_GRAMMARS_SRC
    PromQLLexer.cpp
    PromQLLexer.h
    PromQLParser.cpp
    PromQLParser.h
    PromQLParserBaseListener.cpp
    PromQLParserBaseListener.h
    PromQLParserBaseVisitor.cpp
    PromQLParserBaseVisitor.h
    PromQLParserListener.cpp
    PromQLParserListener.h
    PromQLParserVisitor.cpp
    PromQLParserVisitor.h
)

list(TRANSFORM _ANTLR4_GRAMMARS_SRC PREPEND "${_ANTLR4_GRAMMARS_SOURCE_DIR}/")

add_library(_ch_contrib_antlr4_grammars ${_ANTLR4_GRAMMARS_SRC})

target_include_directories(_ch_contrib_antlr4_grammars
    PUBLIC "${_ANTLR4_GRAMMARS_INCLUDE_DIR}"
)

target_link_libraries(_ch_contrib_antlr4_grammars PUBLIC ch_contrib::antlr4_cpp_runtime)

add_library(ch_contrib::antlr4_grammars ALIAS _ch_contrib_antlr4_grammars)
