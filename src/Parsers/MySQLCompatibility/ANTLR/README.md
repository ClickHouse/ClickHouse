# ANTLR4 MySQL Parser & Lexer

 Currently used antlr version: antlr4.8

## Installation

First, download binaries
```sh
./get_binaries.sh
```

Then compile grammar files into source code
```sh
./gen.sh
```

There will be some compilation errors in `MySQLLexer.cpp` that should be fixed manually. All of them are connected with vectors of UTF-8 strings. They should be replaced by regular std strings, e. g.:
```cpp
std::vector<std::string> 
```

Should become
```cpp
std::vector<std::string> = 
```

Grammar files were got from https://github.com/stevenmiller888/ts-mysql-parser/tree/master/src
