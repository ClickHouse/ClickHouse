-- `arrayJaccardIndex` must compute the union cardinality in the same domain in which `arrayIntersect` compares
-- the elements, i.e. after casting both arguments to their most common subtype. Values that collapse into a single
-- element during that cast must not be counted twice in the denominator.

SELECT 'mixed-scale Decimal';

SELECT arrayJaccardIndex([1.1::Decimal256(1)], [1.10::Decimal256(2), 1.12::Decimal256(2)]);
SELECT arrayJaccardIndex([1.10::Decimal256(2), 1.12::Decimal256(2)], [1.1::Decimal256(1)]);
SELECT round(arrayJaccardIndex([1.1::Decimal256(1), 1.2::Decimal256(1)], [1.10::Decimal256(2), 1.12::Decimal256(2)]), 2);
SELECT arrayJaccardIndex(materialize([1.10::Decimal256(2), 1.12::Decimal256(2)]), [1.1::Decimal256(1)]);

SELECT 'mixed-width integers';

SELECT arrayJaccardIndex([1, 1, 2]::Array(UInt256), [1, 2]::Array(Int128));
SELECT round(arrayJaccardIndex([1, 2, 3]::Array(UInt256), [2, 3, 4]::Array(Int128)), 2);
