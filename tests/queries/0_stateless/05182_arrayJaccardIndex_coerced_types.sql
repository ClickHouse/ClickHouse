-- `arrayJaccardIndex` must return the exact set similarity: the union cardinality counts the distinct values of
-- both arguments as they are, even when the arguments have different types. In particular, values that do not fit
-- into the common subtype that `arrayIntersect` compares in (and are therefore never part of the intersection)
-- are still members of the union, and values that would collapse into one after a lossy cast are still distinct.

SELECT 'integers that overflow the common subtype';

SELECT arrayJaccardIndex([1], [1, 256]);
SELECT arrayJaccardIndex([1], [1, 257]);
SELECT arrayJaccardIndex([1, 256], [1]);
SELECT round(arrayJaccardIndex([1, 256, 257]::Array(UInt16), [1]), 2);
SELECT round(arrayJaccardIndex([1, 256, 256]::Array(UInt16), [1, 1]), 2);
SELECT arrayJaccardIndex(materialize([1, 257]), [1]);
SELECT arrayJaccardIndex(materialize([1]), materialize([1, 257]));
SELECT arrayJaccardIndex([256], [512]);

SELECT 'mixed-scale Decimal';

SELECT arrayJaccardIndex([1.1::Decimal256(1)], [1.10::Decimal256(2), 1.12::Decimal256(2)]);
SELECT arrayJaccardIndex([1.10::Decimal256(2), 1.12::Decimal256(2)], [1.1::Decimal256(1)]);
SELECT arrayJaccardIndex(materialize([1.10::Decimal256(2), 1.12::Decimal256(2)]), [1.1::Decimal256(1)]);

SELECT 'mixed-width integers';

SELECT arrayJaccardIndex([1, 1, 2]::Array(UInt256), [1, 2]::Array(Int128));
SELECT arrayJaccardIndex([1, 2, 3]::Array(UInt256), [2, 3, 4]::Array(Int128));
