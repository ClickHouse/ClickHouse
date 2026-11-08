#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not available in the fast test build.

# Regression test for unbounded recursion in the Parquet V3 native reader's schema converter.
# A chain of nested REQUIRED groups maps to nested Tuples and recurses through
# SchemaConverter::processSubtree per level. The definition-level cap (255) only counts
# OPTIONAL/REPEATED nesting, so a deep REQUIRED chain recursed without bound and overflowed the
# stack -> uncatchable SIGSEGV (server crash / DoS), triggerable even by schema inference (DESC).
# checkStackSize() now turns it into a catchable TOO_DEEP_RECURSION exception.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

# Hand-build a Parquet file (PAR1 + FileMetaData footer, no row groups) whose schema is a root with
# N nested REQUIRED groups (num_children=1 each) and an INT32 leaf. Pure stdlib, no pyarrow.
python3 - "$WORK_DIR" <<'PYEOF'
import json, struct, sys
work = sys.argv[1]

class W:
    def __init__(s): s.out = bytearray()
    def u8(s, v): s.out.append(v & 0xff)
    def varint(s, v):
        v &= (1 << 64) - 1
        while True:
            b = v & 0x7f; v >>= 7
            s.out.append(b | 0x80) if v else s.out.append(b)
            if not v: return
    def zz(s, n): s.varint(((n << 1) ^ (n >> 63)) & ((1 << 64) - 1))

CT_I32, CT_I64, CT_BIN, CT_LIST, CT_STRUCT = 5, 6, 8, 9, 12

def field(w, last, fid, ctype):
    d = fid - last
    if 1 <= d <= 15: w.u8((d << 4) | ctype)
    else: w.u8(ctype); w.zz(fid)
    return fid

def schema_elem(w, name, repetition=None, num_children=None, ptype=None):
    last = 0
    if ptype is not None:
        last = field(w, last, 1, CT_I32); w.zz(ptype)          # type
    if repetition is not None:
        last = field(w, last, 3, CT_I32); w.zz(repetition)     # repetition_type (REQUIRED=0)
    last = field(w, last, 4, CT_BIN); w.varint(len(name)); w.out += name.encode()  # name
    if num_children is not None:
        last = field(w, last, 5, CT_I32); w.zz(num_children)   # num_children
    w.u8(0)

def footer(fmd, last, kv_pairs=None):
    last = field(fmd, last, 3, CT_I64); fmd.zz(0)             # num_rows
    last = field(fmd, last, 4, CT_LIST); fmd.u8((0 << 4) | CT_STRUCT)  # row_groups (empty)
    if kv_pairs:                                              # key_value_metadata list<KeyValue>
        last = field(fmd, last, 5, CT_LIST)
        fmd.u8((len(kv_pairs) << 4) | CT_STRUCT)
        for k, v in kv_pairs:
            kf = field(fmd, 0, 1, CT_BIN); fmd.varint(len(k)); fmd.out += k.encode()
            field(fmd, kf, 2, CT_BIN); fmd.varint(len(v)); fmd.out += v.encode()
            fmd.u8(0)
    fmd.u8(0)
    meta = bytes(fmd.out)
    return b'PAR1' + meta + struct.pack('<I', len(meta)) + b'PAR1'

def header(total):
    fmd = W(); last = 0
    last = field(fmd, last, 1, CT_I32); fmd.zz(2)              # version
    last = field(fmd, last, 2, CT_LIST)                       # schema list<struct>
    if total < 15: fmd.u8((total << 4) | CT_STRUCT)
    else: fmd.u8((15 << 4) | CT_STRUCT); fmd.varint(total)
    return fmd, last

def build(n, children=1, root_children=1):
    fmd, last = header(n + 2)                                 # root + n groups + leaf
    schema_elem(fmd, "root", repetition=None, num_children=root_children)
    for _ in range(n):
        schema_elem(fmd, "g", repetition=0, num_children=children)   # REQUIRED group
    schema_elem(fmd, "leaf", repetition=0, ptype=1)           # INT32 leaf
    return footer(fmd, last)

# GeoParquet metadata naming four covering.bbox sub-columns. The bbox paths come from the FILE,
# not from the query, which only names the geometry column.
GEO = json.dumps({"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
    "encoding": "WKB", "geometry_types": [],
    "covering": {"bbox": {"xmin": ["bbox_xmin"], "ymin": ["bbox_ymin"],
                          "xmax": ["bbox_xmax"], "ymax": ["bbox_ymax"]}}}}}, separators=(",", ":"))

# The same, with TWO-component bbox paths: each is joined with '.', so the names the reader looks
# for are `bbox.xmin` ... and matching them spans a group boundary.
GEO_NESTED = json.dumps({"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
    "encoding": "WKB", "geometry_types": [],
    "covering": {"bbox": {"xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                          "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"]}}}}}, separators=(",", ":"))

def geo_prefix(fmd, bbox_ptype=5):
    """geometry + the four bbox leaves the geo metadata names. bbox_ptype=None leaves them with no
    physical type, which SchemaConverter rejects only for a leaf that is actually REQUESTED."""
    schema_elem(fmd, "geometry", repetition=0, ptype=6)       # BYTE_ARRAY (WKB)
    for nm in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"):
        schema_elem(fmd, nm, repetition=0, ptype=bbox_ptype)  # DOUBLE

def geo_liar(n, children, root_children=6, bbox_ptype=5):
    fmd, last = header(1 + 5 + n + 1)
    schema_elem(fmd, "root", repetition=None, num_children=root_children)
    geo_prefix(fmd, bbox_ptype)
    for _ in range(n):
        schema_elem(fmd, "g", repetition=0, num_children=children)
    schema_elem(fmd, "leaf", repetition=0, ptype=1)
    return footer(fmd, last, [("geo", GEO)])

def geo_comb(d):
    """One primitive leaf at EVERY level, so the set of all leaf paths is ~d^2 bytes while the
    file is ~16*d bytes: root -> g(2) -> L(prim) -> g(2) -> L(prim) -> ... -> g(1) -> L(prim)."""
    fmd, last = header(1 + 5 + 2 * d)
    schema_elem(fmd, "root", repetition=None, num_children=6)
    geo_prefix(fmd)
    for k in range(d):
        schema_elem(fmd, "g", repetition=0, num_children=1 if k == d - 1 else 2)
        schema_elem(fmd, "L", repetition=0, ptype=1)
    return footer(fmd, last, [("geo", GEO)])

def geo_nested(bbox_ptype=None):
    """covering.bbox names `bbox.xmin` ... : root -> geometry + group bbox(4) -> xmin/ymin/xmax/ymax."""
    fmd, last = header(1 + 2 + 4)
    schema_elem(fmd, "root", repetition=None, num_children=2)
    schema_elem(fmd, "geometry", repetition=0, ptype=6)
    schema_elem(fmd, "bbox", repetition=0, num_children=4)
    for nm in ("xmin", "ymin", "xmax", "ymax"):
        schema_elem(fmd, nm, repetition=0, ptype=bbox_ptype)
    return footer(fmd, last, [("geo", GEO_NESTED)])

def geo_decoy():
    """covering.bbox names the FLAT paths, but those four leaves exist only under a long-named
    group, so their real paths are `aaaa....bbox_xmin` and none of the four is a leaf path."""
    fmd, last = header(1 + 2 + 4)
    schema_elem(fmd, "root", repetition=None, num_children=2)
    schema_elem(fmd, "geometry", repetition=0, ptype=6)
    schema_elem(fmd, "aaaaaaaaaaaaaaaaaaaa", repetition=0, num_children=4)
    for nm in ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax"):
        schema_elem(fmd, nm, repetition=0, ptype=5)
    return footer(fmd, last, [("geo", GEO)])

open(f"{work}/deep.parquet", "wb").write(build(50000))
open(f"{work}/shallow.parquet", "wb").write(build(8))
# The root and every group declare INT32_MAX children; no geo metadata.
open(f"{work}/liar.parquet", "wb").write(build(100, 2147483647, 2147483647))
# The same lie, plus covering.bbox metadata, so the schema is searched with a non-empty wanted set.
open(f"{work}/geoliar.parquet", "wb").write(geo_liar(100, 2147483647))
# Only the ROOT lies (INT32_MAX children, 106 elements remain) and the four bbox leaves carry no
# physical type, so the file is readable unless those leaves are requested.
open(f"{work}/georootliar.parquet", "wb").write(geo_liar(100, 1, 2147483647, bbox_ptype=None))
open(f"{work}/geonested.parquet", "wb").write(geo_nested())
open(f"{work}/geodecoy.parquet", "wb").write(geo_decoy())
open(f"{work}/geocomb.parquet", "wb").write(geo_comb(300000))
PYEOF

# Deeply nested schema must be rejected with a catchable error (not crash the process).
out=$(${CLICKHOUSE_LOCAL} --query "
    DESC file('${WORK_DIR}/deep.parquet', Parquet)
    SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
if echo "$out" | grep -q "TOO_DEEP_RECURSION"; then
    echo "deep: rejected"
else
    echo "deep: UNEXPECTED: $out"
fi

# A reasonably nested schema must still work.
shallow=$(${CLICKHOUSE_LOCAL} --query "
    DESC file('${WORK_DIR}/shallow.parquet', Parquet)
    SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
if echo "$shallow" | grep -q "Tuple"; then
    echo "shallow: ok"
else
    echo "shallow: UNEXPECTED: $shallow"
fi

# With a pushed-down filter the reader also searches the schema for the covering.bbox sub-columns,
# before SchemaConverter has validated anything. num_children and the nesting depth come from the
# file, so neither may bound that search: each of the next three files must fail exactly as it does
# with no filter. The oracles grep the error text because error 636 and `timeout` both exit 124.

# Reported shape: root and every group lie about num_children, no geo metadata.
liar=$(timeout 60 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/liar.parquet', Parquet, 'a Int32') WHERE a = 1 FORMAT Null" 2>&1)
if echo "$liar" | grep -q "INCORRECT_DATA"; then
    echo "liar filtered: rejected"
else
    echo "liar filtered: UNEXPECTED: $liar"
fi

POLY="[(-99., 30.), (-96., 30.), (-96., 33.), (-99., 33.), (-99., 30.)]"
# Schema inference cannot run on a malformed footer, hence the explicit structure.
GEO_HINT="geometry Point, bbox_xmin Float64, bbox_ymin Float64, bbox_xmax Float64, bbox_ymax Float64"

# The same lie, with covering.bbox metadata: the spatial filter makes the reader search the schema.
geoliar=$(timeout 60 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/geoliar.parquet', Parquet, '${GEO_HINT}')
    WHERE pointInPolygon(geometry, ${POLY}) FORMAT Null" 2>&1)
if echo "$geoliar" | grep -q "INCORRECT_DATA"; then
    echo "geo liar filtered: rejected"
else
    echo "geo liar filtered: UNEXPECTED: $geoliar"
fi

# One leaf at every level of a 300000-deep schema: the search must not recurse, and must not build
# or remember a path per leaf.
geocomb=$(timeout 120 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/geocomb.parquet', Parquet, '${GEO_HINT}')
    WHERE pointInPolygon(geometry, ${POLY}) FORMAT Null" 2>&1)
if echo "$geocomb" | grep -q "TOO_DEEP_RECURSION"; then
    echo "geo comb filtered: rejected"
else
    echo "geo comb filtered: UNEXPECTED: $geocomb"
fi

# The three arms below check WHICH leaf paths the search returns, not just that it terminates. Each
# fixture names its covering.bbox sub-columns so that the outcome differs depending on whether the
# reader injected them, so an arm fails if the search stops finding paths it should find, or starts
# finding paths the file does not contain. None of them names a bbox column in the structure hint:
# injection is the only way those columns can enter the block.

# Only the ROOT lies about num_children. Its four bbox leaves have no physical type, which is an
# error only for a leaf that is requested, so this arm passes only if the search matched them while
# the root declared INT32_MAX children: clamping that count is not the same as abandoning the search.
georootliar=$(timeout 120 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/georootliar.parquet', Parquet, 'geometry Point')
    WHERE pointInPolygon(geometry, ${POLY})
    SETTINGS input_format_parquet_spatial_filter_push_down = 1 FORMAT Null" 2>&1)
if echo "$georootliar" | grep -qF "missing physical type for column bbox_xmin"; then
    echo "geo root liar filtered: rejected"
else
    echo "geo root liar filtered: UNEXPECTED: $georootliar"
fi

# covering.bbox names `bbox.xmin` ... , so a match spans a group boundary and the separator is part
# of the name being matched. Nothing else in the tree distinguishes a per-component match from a
# per-character one. Both the old and the new search find these, so this arm guards equivalence; it
# is not a denial-of-service arm.
geonested=$(timeout 120 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/geonested.parquet', Parquet, 'geometry Point')
    WHERE pointInPolygon(geometry, ${POLY})
    SETTINGS input_format_parquet_spatial_filter_push_down = 1 FORMAT Null" 2>&1)
if echo "$geonested" | grep -qF "missing physical type for column bbox.xmin"; then
    echo "geo nested bbox filtered: rejected"
else
    echo "geo nested bbox filtered: UNEXPECTED: $geonested"
fi

# The four names the geo metadata gives are not leaf paths here: they exist only below a long-named
# group. Matching one anyway would inject a column the file does not have, which
# allow_missing_columns = 0 turns into an error instead of a silently defaulted column.
geodecoy=$(timeout 120 ${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${WORK_DIR}/geodecoy.parquet', Parquet, 'geometry Point')
    WHERE pointInPolygon(geometry, ${POLY})
    SETTINGS input_format_parquet_spatial_filter_push_down = 1,
             input_format_parquet_allow_missing_columns = 0 FORMAT Null" 2>&1)
if [ -z "$geodecoy" ]; then
    echo "geo absent bbox filtered: ok"
else
    echo "geo absent bbox filtered: UNEXPECTED: $geodecoy"
fi
