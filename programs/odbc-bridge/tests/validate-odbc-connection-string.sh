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
./validate-odbc-connection-string 'DSN=myconnection' 2>&1
./validate-odbc-connection-string 'DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db' 2>&1
./validate-odbc-connection-string 'DSN=MSSQL;UID=test;PWD=test' 2>&1
