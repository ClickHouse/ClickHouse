#!/bin/bash -eu
# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################

# Fuzzer runner. Appends .options arguments and seed corpus to users args.
# Usage: $0 <fuzzer_name> <fuzzer_args>

export PATH=$OUT:$PATH
cd $OUT

DEBUGGER=${DEBUGGER:-}

FUZZER=$1
shift

# This env var is set by CIFuzz. CIFuzz fills this directory with the corpus
# from ClusterFuzz.
CORPUS_DIR=${CORPUS_DIR:-}
if [ -z "$CORPUS_DIR" ]
then
  CORPUS_DIR="/tmp/${FUZZER}_corpus"
  rm -rf $CORPUS_DIR && mkdir -p $CORPUS_DIR
fi

SANITIZER=${SANITIZER:-}
if [ -z $SANITIZER ]; then
  # If $SANITIZER is not specified (e.g. calling from `reproduce` command), it
  # is not important and can be set to any value.
  SANITIZER="default"
fi

if [[ "$RUN_FUZZER_MODE" = interactive ]]; then
  FUZZER_OUT="$OUT/${FUZZER}_${FUZZING_ENGINE}_${SANITIZER}_out"
else
  FUZZER_OUT="/tmp/${FUZZER}_${FUZZING_ENGINE}_${SANITIZER}_out"
fi


rm -rf $FUZZER_OUT && mkdir -p $FUZZER_OUT

SEED_CORPUS="${FUZZER}_seed_corpus.zip"

# TODO: Investigate why this code block is skipped
# by all default fuzzers in bad_build_check.
# They all set SKIP_SEED_CORPUS=1.
if [ -f $SEED_CORPUS ] && [ -z ${SKIP_SEED_CORPUS:-} ]; then
  echo "Using seed corpus: $SEED_CORPUS"
  unzip -o -d ${CORPUS_DIR}/ $SEED_CORPUS > /dev/null
fi

OPTIONS_FILE="${FUZZER}.options"
CUSTOM_LIBFUZZER_OPTIONS=""

if [ -f $OPTIONS_FILE ]; then
  custom_asan_options=$(parse_options.py $OPTIONS_FILE asan)
  if [ ! -z $custom_asan_options ]; then
    export ASAN_OPTIONS="$ASAN_OPTIONS:$custom_asan_options"
  fi

  custom_msan_options=$(parse_options.py $OPTIONS_FILE msan)
  if [ ! -z $custom_msan_options ]; then
    export MSAN_OPTIONS="$MSAN_OPTIONS:$custom_msan_options"
  fi

  custom_ubsan_options=$(parse_options.py $OPTIONS_FILE ubsan)
  if [ ! -z $custom_ubsan_options ]; then
    export UBSAN_OPTIONS="$UBSAN_OPTIONS:$custom_ubsan_options"
  fi

  CUSTOM_LIBFUZZER_OPTIONS=$(parse_options.py $OPTIONS_FILE libfuzzer)
fi



CMD_LINE="$OUT/$FUZZER $FUZZER_ARGS $*"

if [ -z ${SKIP_SEED_CORPUS:-} ]; then
CMD_LINE="$CMD_LINE $CORPUS_DIR"
fi

if [[ ! -z ${CUSTOM_LIBFUZZER_OPTIONS} ]]; then
CMD_LINE="$CMD_LINE $CUSTOM_LIBFUZZER_OPTIONS"
fi

if [[ ! "$CMD_LINE" =~ "-dict=" ]]; then
if [ -f "$FUZZER.dict" ]; then
    CMD_LINE="$CMD_LINE -dict=$FUZZER.dict"
fi
fi

CMD_LINE="$CMD_LINE < /dev/null"

echo $CMD_LINE

# Unset OUT so the fuzz target can't rely on it.
unset OUT

if [ ! -z "$DEBUGGER" ]; then
  CMD_LINE="$DEBUGGER $CMD_LINE"
fi

bash -c "$CMD_LINE"
