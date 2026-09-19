#!/bin/bash -eu

# copy fuzzer options and dictionaries
cp $SRC/tests/fuzz/*.dict $OUT/
cp $SRC/tests/fuzz/*.options $OUT/

# prepare corpus dirs
mkdir -p $BIN/tests/fuzz/lexer_fuzzer.in/
mkdir -p $BIN/tests/fuzz/select_parser_fuzzer.in/
mkdir -p $BIN/tests/fuzz/create_parser_fuzzer.in/

# prepare corpus
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/lexer_fuzzer.in/
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/select_parser_fuzzer.in/
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/create_parser_fuzzer.in/

# committed binary seed corpora (protobuf inputs cannot be derived from SQL files at build time)
cp -r $SRC/tests/fuzz/json_ast_sql_parser_fuzzer.in $BIN/tests/fuzz/
# the execution fuzzer consumes the same protobuf inputs
cp -r $SRC/tests/fuzz/json_ast_sql_parser_fuzzer.in $BIN/tests/fuzz/json_ast_sql_execution_fuzzer.in

# build corpus archives
cd $BIN/tests/fuzz
for dir in *_fuzzer.in; do
    fuzzer=$(basename $dir .in)
    zip -rj "$OUT/${fuzzer}_seed_corpus.zip" "${dir}/"
done
