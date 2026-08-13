-- Comparison of Array(FixedString(16)) against Array(IPv6) (in either order) must be executable
-- element-wise, mirroring the dedicated FixedString(16) <-> IPv6 full-column path used for the
-- scalar and tuple cases. Previously the array (and the scalar null-safe) forms wrongly threw
-- ILLEGAL_TYPE_OF_ARGUMENT.

-- Scalar reference (already worked for regular =, now also for the null-safe operator).
SELECT IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))::FixedString(16) = toIPv6('::ffff:127.0.0.1')::IPv6;
SELECT IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))::FixedString(16) IS NOT DISTINCT FROM toIPv6('::ffff:127.0.0.1')::IPv6;

-- Array, regular comparison operators, both operand orders.
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) = [toIPv6('::ffff:127.0.0.1')]::Array(IPv6);
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.2'))]::Array(FixedString(16)) = [toIPv6('::ffff:127.0.0.1')]::Array(IPv6);
SELECT [toIPv6('::ffff:127.0.0.1')]::Array(IPv6) = [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16));
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) != [toIPv6('::ffff:127.0.0.1')]::Array(IPv6);
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) < [toIPv6('::ffff:127.0.0.2')]::Array(IPv6);

-- Array, null-safe comparison operators.
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) IS DISTINCT FROM [toIPv6('::ffff:127.0.0.1')]::Array(IPv6);
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) IS NOT DISTINCT FROM [toIPv6('::ffff:127.0.0.1')]::Array(IPv6);

-- Genuinely unsupported string-vs-non-string element pairs must still be rejected.
SELECT ['1']::Array(String) = [1]::Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) = [1]::Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- A FixedString whose size is not exactly the IPv6 binary length (16) is not a valid IPv6
-- representation, including sizes adjacent to 16.
SELECT ['abcd']::Array(FixedString(4)) = [toIPv6('::1')]::Array(IPv6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [repeat('x', 15)]::Array(FixedString(15)) = [toIPv6('::1')]::Array(IPv6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT [repeat('x', 17)]::Array(FixedString(17)) = [toIPv6('::1')]::Array(IPv6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- The exemption is IPv6-specific: FixedString(16) vs IPv4 remains unsupported element-wise.
SELECT [IPv4ToIPv6(IPv4StringToNum('127.0.0.1'))]::Array(FixedString(16)) = [toIPv4('1.2.3.4')]::Array(IPv4); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
