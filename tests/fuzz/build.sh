#!/bin/bash -eu

# copy fuzzer options and dictionaries
#
# all.dict is not committed: the fuzzing configure step generates a
# source-derived fallback into tests/fuzz/, and the nightly libFuzzer job
# overwrites it with the authoritative binary-derived one (update_dict.sh).
# Either way it is present here, so copy whichever is current instead of
# generating a second one.
cp $SRC/tests/fuzz/*.dict $OUT/
cp $SRC/tests/fuzz/*.options $OUT/

# prepare corpus dirs
mkdir -p $BIN/tests/fuzz/lexer_fuzzer.in/
mkdir -p $BIN/tests/fuzz/select_parser_fuzzer.in/
mkdir -p $BIN/tests/fuzz/create_parser_fuzzer.in/
mkdir -p $BIN/tests/fuzz/execute_query_fuzzer.in/

# prepare corpus
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/lexer_fuzzer.in/
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/select_parser_fuzzer.in/
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/create_parser_fuzzer.in/
cp $SRC/tests/queries/0_stateless/*.sql $BIN/tests/fuzz/execute_query_fuzzer.in/

# build corpus archives
cd $BIN/tests/fuzz
for dir in *_fuzzer.in; do
    fuzzer=$(basename $dir .in)
    zip -rj "$OUT/${fuzzer}_seed_corpus.zip" "${dir}/"
done
