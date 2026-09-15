#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Cap'n Proto schema parser is recursive, so a deeply nested type expression used to exhaust
# the thread stack while the schema was being parsed.

DEPTH=20000
NESTED="$(python3 -c "print('List(' * $DEPTH + 'Int32' + ')' * $DEPTH)")"

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '')
SETTINGS
    format_schema_source = 'string',
    format_schema = '@0x844f048b15c12dab;\nstruct M { data @0 :${NESTED}; }',
    format_schema_message_name = 'M'
" 2>&1 | grep -c -F 'nested too deeply'

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '')
SETTINGS
    format_schema_source = 'string',
    format_schema = '@0x844f048b15c12dab;\nstruct M { data @0 :List(List(Int32)); }',
    format_schema_message_name = 'M'
"

# The schema files that a schema `import`s are parsed by the same recursive parser, so a shallow
# entry schema must not be able to smuggle a deeply nested one in through an import.
# `format_schema` with a file must be given an absolute path, and the directory has to be unique
# per test run, because the flaky check runs this test many times concurrently.
SCHEMA_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/05034_capnproto_schemas_XXXXXX")"
SCHEMA_DIR="$(cd "${SCHEMA_DIR}" && pwd)"

python3 -c "
import sys
directory = sys.argv[1]
depth = int(sys.argv[2])
with open(directory + '/deep.capnp', 'w') as f:
    f.write('@0x844f048b15c12dac;\nstruct D { data @0 :' + 'List(' * depth + 'Int32' + ')' * depth + '; }\n')
with open(directory + '/imports_deep.capnp', 'w') as f:
    f.write('@0x844f048b15c12dab;\nusing D = import \"deep.capnp\";\nstruct M { data @0 :D.D; }\n')
with open(directory + '/shallow.capnp', 'w') as f:
    f.write('@0x844f048b15c12dad;\nstruct S { data @0 :List(List(Int32)); }\n')
with open(directory + '/imports_shallow.capnp', 'w') as f:
    f.write('@0x844f048b15c12dae;\nusing S = import \"shallow.capnp\";\nstruct M { data @0 :S.S; }\n')
" "${SCHEMA_DIR}" "${DEPTH}"

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/imports_deep.capnp:M'
" 2>&1 | grep -c -F 'nested too deeply'

${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/imports_shallow.capnp:M'
"

rm -rf "${SCHEMA_DIR}"
