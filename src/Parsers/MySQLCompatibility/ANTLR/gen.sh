#!/bin/bash

java -jar ./antlr.jar -Dlanguage=Cpp MySQLLexer.g4
java -jar ./antlr.jar -Dlanguage=Cpp MySQLParser.g4

# fix lexer compilation errors
sed -i 's/u8//g' MySQLLexer.cpp
