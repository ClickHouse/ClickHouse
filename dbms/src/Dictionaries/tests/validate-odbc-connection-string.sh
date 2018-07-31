#!/bin/sh

./validate-odbc-connection-string '' 2>&1
./validate-odbc-connection-string 'abc' 2>&1
./validate-odbc-connection-string 'abc=' 2>&1
./validate-odbc-connection-string 'ab"c=' 2>&1
./validate-odbc-connection-string 'abc=def' 2>&1
./validate-odbc-connection-string 'abc=de[f' 2>&1
./validate-odbc-connection-string 'abc={de[f}' 2>&1
./validate-odbc-connection-string 'abc={de[f};dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}}f};dsn=hello' 2>&1
./validate-odbc-connection-string 'abc=de}}f};dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}}f;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}f;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}f;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}f};dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}}f};dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}}f;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={de}} ;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={  } ;dsn=hello' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn=hello   ' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn=hello world  ' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ...' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;...' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;=' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_=' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= ' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {}' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {}}}' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {...................................................................}' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {....................................................................................}' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {.....................................................................................................}' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {...}; FILEDSN=x' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {...}; FileDsn  = x' 2>&1
./validate-odbc-connection-string 'abc={  } ; dsn = {hello world}  ;_= {...}; Driver=x' 2>&1
./validate-odbc-connection-string 'abc={}; abc=def' 2>&1
./validate-odbc-connection-string 'abc={};;' 2>&1
