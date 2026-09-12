#!/usr/bin/env bash
# Tags: no-fasttest

# Union mode merges the per-file schemas through a fresh stateless schema reader, so the per-file
# inference provenance (which Int64 was inferred from a negative literal) is gone by then. Without it a
# sign-dependent Int64 to UInt64 widening cannot be proven safe, so it is declined and the caller
# reports the type mismatch instead of inferring a type whose read then fails. Needs real files, which
# is why this is a shell test and not part of 04653.
#
# The Template section at the end is here for the same reason: the Template INPUT format takes its row
# format only as a file path, so it needs a schema file that a .sql test cannot write.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR=$CLICKHOUSE_TEST_UNIQUE_NAME
rm -rf "$DIR"
mkdir -p "$DIR"

printf 'x=-1\n' > "$DIR/neg.tskv"
printf 'x=-2\n' > "$DIR/neg2.tskv"
printf 'x=1\n' > "$DIR/pos.tskv"
printf 'x=18446744073709551615\n' > "$DIR/big.tskv"
printf 'x=1.5\n' > "$DIR/float.tskv"
printf 'x=\\N\nx=18446744073709551615\n' > "$DIR/nullbig.tskv"
printf 'x=[1]\n' > "$DIR/arr.tskv"
printf "x=(1,'a')\n" > "$DIR/tup.tskv"
printf 'x=abc\n' > "$DIR/str.tskv"
printf 'x=[-1]\n' > "$DIR/negarr.tskv"
printf 'x=[18446744073709551615]\n' > "$DIR/bigarr.tskv"

# Report either the inferred type or the error name, never both: the error message text also contains a
# type name, and it carries the temporary file path, which is not reproducible.
verdict() {
    local out
    out=$($CLICKHOUSE_LOCAL -m -q "$1" 2>&1)
    if echo "$out" | grep -q 'TYPE_MISMATCH'; then
        echo "TYPE_MISMATCH"
    elif echo "$out" | grep -q 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'; then
        echo "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
    else
        echo "$out" | cut -f2- | tr -d '\t'
    fi
}

echo "1. a negative integer and a UInt64-range value in separate files"
# Not a schema whose read then fails: a loud refusal at inference time is the acceptable outcome.
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,big}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,big}.tskv', TSKV) order by tuple(*);"

echo "2. the same shape without a negative value is refused identically, as before this change"
verdict "set schema_inference_mode='union'; desc file('$DIR/{pos,big}.tskv', TSKV);"

echo "3. merges that do not depend on the sign of an integer still happen"
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,neg2}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,neg2}.tskv', TSKV) order by tuple(*);"
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,float}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,float}.tskv', TSKV) order by tuple(*);"

echo "4. the default (non-union) mode on the same two files is unaffected"
verdict "set schema_inference_mode='default'; desc file('$DIR/{neg,big}.tskv', TSKV);"
verdict "set schema_inference_mode='default'; select * from file('$DIR/{neg,big}.tskv', TSKV) order by tuple(*);"

# schema_inference_make_columns_nullable=2 is the load-bearing value: it is the only one that leaves the
# per-file type unwrapped, so a file containing \N yields Nullable(UInt64) while a file without one
# yields a bare Int64, and only then does the merge see an asymmetrically wrapped pair. Comparing the
# wrapped shapes made the sign check give up, so the widening it exists to decline went through.
echo "5. asymmetric nullability across files does not get past the sign check"
verdict "set schema_inference_mode='union', schema_inference_make_columns_nullable=2; desc file('$DIR/{neg,nullbig}.tskv', TSKV);"
verdict "set schema_inference_mode='union', schema_inference_make_columns_nullable=2; select * from file('$DIR/{neg,nullbig}.tskv', TSKV) order by tuple(*);"
# The opposite file order reaches the merge with the two types swapped, and used to emit rows before failing.
verdict "set schema_inference_mode='union', schema_inference_make_columns_nullable=2; desc file('$DIR/{nullbig,neg}.tskv', TSKV);"
verdict "set schema_inference_mode='union', schema_inference_make_columns_nullable=2; select * from file('$DIR/{nullbig,neg}.tskv', TSKV) order by tuple(*);"
# The same shape without a negative value is refused identically, as it is on master.
verdict "set schema_inference_mode='union', schema_inference_make_columns_nullable=2; desc file('$DIR/{pos,nullbig}.tskv', TSKV);"

# Declining the merge is only correct where the sign of an integer is actually at stake. Column shapes
# that simply differ are unified into a Variant when that is enabled, and the separate-file answer must
# be the same as the single-file one, which is why each pair below is asserted both ways.
echo "6. column shapes that differ without a sign hazard are still merged"
verdict "set schema_inference_mode='union', input_format_try_infer_variants=1; desc file('$DIR/{arr,tup}.tskv', TSKV);"
verdict "set input_format_try_infer_variants=1; desc format(TSKV, \$\$x=[1]
x=(1,'a')
\$\$);"
verdict "set schema_inference_mode='union', input_format_try_infer_variants=1; desc file('$DIR/{arr,str}.tskv', TSKV);"
verdict "set input_format_try_infer_variants=1; desc format(TSKV, \$\$x=[1]
x=abc
\$\$);"
# An Int64 and a UInt64 in non-corresponding positions are never paired by the transformation either,
# because it stops descending where the container kinds diverge. So there is no widening to decline and
# this pair merges like the two above, again agreeing with the single-file answer.
verdict "set schema_inference_mode='union', input_format_try_infer_variants=1; desc file('$DIR/{negarr,big}.tskv', TSKV);"
verdict "set input_format_try_infer_variants=1; desc format(TSKV, \$\$x=[-1]
x=18446744073709551615
\$\$);"
# The same two integers at the SAME position are paired, so that pair keeps the sign hazard and is
# still declined: the narrowing above must not let it through.
verdict "set schema_inference_mode='union', input_format_try_infer_variants=1; desc file('$DIR/{negarr,bigarr}.tskv', TSKV);"

# Template is the last escaped-rule reader with its own override and its own provenance set. Its input
# format takes the row format only as a file path, so unlike the CustomSeparated and Regexp cases in
# 04653 it needs a schema file. Reading the values back is the point: without the fix DESC proposes an
# unsigned type and the read of the negative row then hard-errors.
# An absolute format_schema is accepted because these run through clickhouse-local, so the file lives in
# this test's own directory: nothing shared, and it is removed with the rest at the end.
ROW_FORMAT=$(pwd)/$DIR/row_format
echo -e "\${c1:Escaped}" > "$ROW_FORMAT"
TEMPLATE_SETTINGS="format_template_row='$ROW_FORMAT', format_template_rows_between_delimiter=''"

echo "7. Template, the remaining escaped-rule reader, is order-independent too"
# 1 / -1 / 18446744073709551615
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('310A2D310A31383434363734343037333730393535313631350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('310A2D310A31383434363734343037333730393535313631350A'));"
# -1 / 1 / 18446744073709551615 - the opposite order must agree
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('2D310A310A31383434363734343037333730393535313631350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('2D310A310A31383434363734343037333730393535313631350A'));"
# 1 / 2 / 18446744073709551615 - no negative value, so the widening must still happen
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('310A320A31383434363734343037333730393535313631350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('310A320A31383434363734343037333730393535313631350A'));"

# A signed zero is the same hazard with a value that is not negative, so it pins that the recorded
# property is the written sign. Both file orders, then the same shape with an unsigned literal.
# -0 / 18446744073709551615
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('2D300A31383434363734343037333730393535313631350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('2D300A31383434363734343037333730393535313631350A'));"
# 18446744073709551615 / -0 - the opposite order must agree
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('31383434363734343037333730393535313631350A2D300A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('31383434363734343037333730393535313631350A2D300A'));"
# 1 / 18446744073709551615 - no sign, so the widening must still happen
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('310A31383434363734343037333730393535313631350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('310A31383434363734343037333730393535313631350A'));"

# A single row carrying an explicit plus has no merge to decline, so its own type has to be readable on
# its own. The escaped-rule value reader refuses a '+', so it falls back to String as in 04653 group 13e.
# Floats do consume a '+' and keep Float64; a '-' is consumed too and needs no fallback.
# +1
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('2B310A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('2B310A'));"
# +1.5
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('2B312E350A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('2B312E350A'));"
# -1
verdict "set $TEMPLATE_SETTINGS; desc format(Template, unhex('2D310A'));"
verdict "set $TEMPLATE_SETTINGS; select * from format(Template, unhex('2D310A'));"

rm -rf "$DIR"
