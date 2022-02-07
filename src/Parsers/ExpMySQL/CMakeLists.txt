add_library(
    ExpMySQL
    MySQLLexer.h
    MySQLLexer.cpp
    MySQLParser.h
    MySQLParser.cpp
    MySQLBaseLexer.h
    MySQLBaseParser.h
    SqlMode.h
)

message(STATUS "TEST TEST TEST")

target_compile_options(ExpMySQL PRIVATE -Wno-unused-result -Wno-reserved-id-macro -Wno-disabled-macro-expansion)


target_link_libraries(ExpMySQL)
