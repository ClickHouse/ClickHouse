#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_TMP_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${TEST_TMP_DIR}"

clang-${LLVM_VERSION} -std=c23 --target=wasm32 -msimd128 -ffreestanding -nostdlib -Oz -c "${CUR_DIR}/wasm/mobius.c" -o "${TEST_TMP_DIR}/mobius.o"
wasm-ld-${LLVM_VERSION} --export-all --no-entry --lto-O3 --allow-undefined "${TEST_TMP_DIR}/mobius.o" -o "${TEST_TMP_DIR}/mobius.wasm"

du "${TEST_TMP_DIR}/mobius.wasm"
md5sum "${TEST_TMP_DIR}/mobius.wasm"
cat "${TEST_TMP_DIR}/mobius.wasm" | hexdump -C

rm -rf "${TEST_TMP_DIR}"
