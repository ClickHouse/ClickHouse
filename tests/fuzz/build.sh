#!/bin/bash -eu

# copy fuzzer options
cp $SRC/tests/fuzz/*.options $OUT/

# Some .options files reference all.dict. The authoritative all.dict is
# generated at test time from a release binary (see tests/fuzz/update_dict.sh
# and ci/jobs/libfuzzer_test_check.py) and overrides this one; stage a
# source-derived dictionary for consumers that run the fuzzers straight from
# the build output (local runs, OSS-Fuzz), where no binary-derived dictionary
# is available.
"$SRC/tests/fuzz/generate_source_dict.sh" "$SRC" "$OUT/all.dict"

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
