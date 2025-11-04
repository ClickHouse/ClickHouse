CREATE TEMPORARY TABLE _03212_ip_table ENGINE = Memory AS
WITH
    'Variant(String, UInt32)' AS VTYPE,
    -- 4 invalid strings that toIPv4OrDefault() maps to 0.0.0.0
    ['bad.ip.aaa', '300.300.300.300', 'abc', ''] AS bad_strs,
    -- 2 numeric IPv4s as UInt32
    [toUInt32(117911771), toUInt32(3232235521)] AS nums_v4
SELECT arrayJoin(
           arrayConcat(
               arrayMap(s -> CAST(s, VTYPE), bad_strs),
               arrayMap(x -> CAST(x, VTYPE), nums_v4)
           )
       ) AS v;

-- numeric reinterpret is stable and equals CAST.
WITH v, variantElement(v,'UInt32') AS u32_n
SELECT
    'VARIANT_IPV4_NUMERIC_EQUALS' AS tag,
    countIf(isNotNull(u32_n))     AS numeric_rows,     -- expect 2
    countIf(isNotNull(u32_n) AND CAST(v,'IPv4') = toIPv4(assumeNotNull(u32_n))) AS equals_count  -- expect 2
FROM _03212_ip_table
FORMAT TSV;

-- String exists AND would default → CAST must throw 675.
SELECT CAST(v,'IPv4')
FROM _03212_ip_table
WHERE isNotNull(variantElement(v,'String'))
  AND toIPv4OrDefault(assumeNotNull(variantElement(v,'String'))) = toIPv4('0.0.0.0')
FORMAT Null; -- { serverError 675 }
