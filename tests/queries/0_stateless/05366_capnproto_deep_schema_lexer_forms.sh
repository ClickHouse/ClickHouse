#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The CapnProto schema nesting depth check must see the same tokens as the Cap'n Proto lexer: comments and byte order
# marks between `import` and its path, escape sequences in a quoted path, and backtick text lines.

guard_message_count() {
    local output count
    output=$(cat)
    count=$(grep -c -F 'nested too deeply' <<< "$output") || true
    echo "$count"
    [ "$count" = 1 ] || echo "FAIL: the depth guard did not fire, got: $(head -c 1000 <<< "$output" | tr '\n' ' ')"
}

SCHEMA_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/capnproto_schemas_XXXXXX")"
SCHEMA_DIR="$(cd "${SCHEMA_DIR}" && pwd)"
trap 'rm -rf "${SCHEMA_DIR}"' EXIT

python3 - "${SCHEMA_DIR}" <<'PY'
import sys
directory = sys.argv[1]
nested = 'List(' * 20000 + 'Int32' + ')' * 20000

def schema(name, text):
    with open(directory + '/' + name, 'w', encoding='utf-8', newline='') as f:
        f.write(text)

schema('deep.capnp', '@0x844f048b15c12dac;\nstruct D { data @0 :' + nested + '; }\n')
# The value of a backtick text literal ends with a line break, also when it is the path of an import.
schema('deep_backtick.capnp\n', '@0x844f048b15c12dad;\nstruct D { data @0 :' + nested + '; }\n')

schema('import_after_comment.capnp', '@0x844f048b15c12dab;\nusing D = import # c\n"deep.capnp";\nstruct M { data @0 :D.D; }\n')
schema('import_after_bom.capnp', '@0x844f048b15c12dab;\nusing D = import\ufeff"deep.capnp";\nstruct M { data @0 :D.D; }\n')
schema('import_escaped.capnp', '@0x844f048b15c12dab;\nusing D = import "deep\\x2ecapnp";\nstruct M { data @0 :D.D; }\n')
schema('import_backtick.capnp', '@0x844f048b15c12dab;\nusing D = import `deep_backtick.capnp\n;\nstruct M { data @0 :D.D; }\n')
schema('quote_in_backtick.capnp', '@0x844f048b15c12dab;\nconst a :Text = `"\n;\nstruct M { data @0 :' + nested + '; }\n')
schema('hash_in_backtick.capnp', '@0x844f048b15c12dab;\nconst a :Text = `#\r;struct M { data @0 :' + nested + '; }\n')
schema('brackets_in_backtick.capnp', '@0x844f048b15c12dab;\nconst a :Text = `' + '(' * 1001 + '\n;\nstruct M { data @0 :Int32; }\n')
schema('data_literal_with_line_break.capnp', '@0x844f048b15c12dab;\nconst a :Data = 0x"62\n61";\nstruct M { data @0 :' + nested + '; }\n')
PY

for name in import_after_comment import_after_bom import_escaped import_backtick quote_in_backtick hash_in_backtick data_literal_with_line_break
do
    echo "${name}"
    ${CLICKHOUSE_LOCAL} --logger.console=0 --query "
    DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/${name}.capnp:M'
    " 2>&1 | guard_message_count
done

# Brackets in a backtick text line are text, not nesting.
${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/brackets_in_backtick.capnp:M'
"
