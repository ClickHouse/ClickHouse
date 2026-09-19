#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `Arrow` format is not available in the fasttest environment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A struct with no fields at all infers as `Tuple()`, and when the header is taken from such a file
# while another file of the same glob carries a struct with fields, the reader has to realign the
# decoded struct to a requested tuple without a single element.
#
# The glob listing is not ordered, so the header may come from either file. The two files carry
# the shapes swapped between columns `a` and `b`: whichever file supplies the header, one column
# is a field-less requested tuple against a decoded struct with a field, and the other one is a
# requested tuple with a field against a decoded field-less struct (filled with defaults).
# Only order-independent facts are asserted for the glob read.

DIR="${CLICKHOUSE_TMP}/05055_arrow_ipc_empty_struct"
rm -rf "${DIR}"
mkdir -p "${DIR}"

${CLICKHOUSE_LOCAL} --query "SELECT number::Int32 AS n, tuple()::Tuple() AS a, tuple(number::Int32)::Tuple(x Int32) AS b FROM numbers(3) INTO OUTFILE '${DIR}/1.arrow' FORMAT Arrow"
${CLICKHOUSE_LOCAL} --query "SELECT (number + 10)::Int32 AS n, tuple(number::Int32)::Tuple(x Int32) AS a, tuple()::Tuple() AS b FROM numbers(3) INTO OUTFILE '${DIR}/2.arrow' FORMAT Arrow"

${CLICKHOUSE_LOCAL} --query "SELECT count(), sum(n), sum(ignore(a, b)), length(tupleNames(any(a))) + length(tupleNames(any(b))) AS elements_in_header FROM file('${DIR}/*.arrow', Arrow)"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DIR}/1.arrow', Arrow) ORDER BY n"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DIR}/2.arrow', Arrow) ORDER BY n"

rm -rf "${DIR}"
