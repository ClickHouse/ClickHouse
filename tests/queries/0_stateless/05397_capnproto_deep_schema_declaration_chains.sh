#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Long chains of type expression suffixes or of declarations that refer to each other have no deep bracket nesting,
# but the Cap'n Proto schema compiler recurses once per link. They must be rejected, not overflow the stack.

guard_message_count() {
    local output count
    output=$(cat)
    count=$(grep -c -E 'Code: 622\..*nested too deeply' <<< "$output") || true
    echo "$count"
    [ "$count" = 1 ] || echo "FAIL: the depth guard did not fire, got: $(head -c 1000 <<< "$output" | tr '\n' ' ')"
}

SCHEMA_DIR="$(mktemp -d "${CLICKHOUSE_TMP}/capnproto_schemas_XXXXXX")"
SCHEMA_DIR="$(cd "${SCHEMA_DIR}" && pwd)"
trap 'rm -rf "${SCHEMA_DIR}"' EXIT

python3 - "${SCHEMA_DIR}" <<'PY'
import sys
directory = sys.argv[1]
header = '@0x844f048b15c12dab;\n'
message = 'struct M { data @0 :Int32; }\n'

def alias_chain(n):
    return ''.join(f'using A{i} = A{i + 1};\n' for i in range(n)) + f'using A{n} = Int32;\n'

def const_chain(n):
    return ''.join(f'const c{i} :Int32 = .c{i + 1};\n' for i in range(n)) + f'const c{n} :Int32 = 1;\n'

def struct_chain(n):
    return ''.join(f'struct S{i} {{ f @0 :S{i + 1}; }}\n' for i in range(n)) + f'struct S{n} {{ x @0 :Int32; }}\n'

# P's default values load every S{i} first, so only walking the chain from P.d0 goes deep.
def struct_chain_with_defaults(n):
    return 'struct P { ' + ' '.join(f'd{i} @{i} :S{i} = ();' for i in range(n + 1)) + ' }\n' + struct_chain(n)

def enum_annotation_chain(n):
    return ''.join(f'annotation t{i}(*) :E{i + 1};\nenum E{i} {{ v @0 $t{i}(v); }}\n' for i in range(n)) + f'enum E{n} {{ v @0; }}\n'

def schema(name, text):
    with open(directory + '/' + name + '.capnp', 'w') as f:
        f.write(header + text)

# Long enough to overflow the stack without the guard, and several times the length at which a release build rejects them.
n = 5000
n_suffix = 20000
schema('member_chain', 'struct M { data @0 :Int32' + '.a' * n_suffix + '; }\n')
schema('application_chain', 'struct M { data @0 :Int32' + '()' * n_suffix + '; }\n')
schema('nested_suffix_chains', 'struct M { data @0 :' + 'List(' * 10 + 'Int32' + (')' + '.a' * 200) * 10 + '; }\n')
schema('alias_chain', 'struct M { data @0 :A0; }\n' + alias_chain(n))
schema('const_chain', message + const_chain(n))
# A struct field costs less stack per link than the chains above.
schema('struct_chain_with_defaults', message + struct_chain_with_defaults(12000))
schema('enum_annotation_chain', message + enum_annotation_chain(n))
schema('shallow_chains', 'struct M { data @0 :A0; s @1 :S0; e @2 :E0; }\n'
       + alias_chain(2) + const_chain(2) + struct_chain_with_defaults(2) + enum_annotation_chain(2))
PY

for name in member_chain application_chain nested_suffix_chains alias_chain const_chain struct_chain_with_defaults enum_annotation_chain
do
    echo "${name}"
    ${CLICKHOUSE_LOCAL} --logger.console=0 --query "
    DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/${name}.capnp:M'
    " 2>&1 | guard_message_count
done

# Short chains of the same shapes are valid schemas.
${CLICKHOUSE_LOCAL} --logger.console=0 --query "
DESC format(CapnProto, '') SETTINGS format_schema = '${SCHEMA_DIR}/shallow_chains.capnp:M'
"
