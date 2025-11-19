SET allow_experimental_qbit_type = 1;

SELECT 'Test Array → QBit CAST AS: Float64';

SELECT CAST((SELECT groupArray(number + 0.1) FROM numbers(2)) AS QBit(Float64, 2));
SELECT CAST((SELECT groupArray(number + 0.1) FROM numbers(2)) AS QBit(Float64, 2));
SELECT CAST((SELECT groupArray(number + 0.1) FROM numbers(2)) AS QBit(Float64, 2));
SELECT * FROM format('Values', 'qbit QBit(Float64, 2)', '(array(0.1,1.1))');


SELECT 'Test Array → QBit CAST AS: Float32';
SELECT CAST((SELECT groupArray(toFloat32(number + 0.1)) FROM numbers(2)) AS QBit(Float32, 2));
SELECT CAST((SELECT groupArray(toFloat32(number + 0.1)) FROM numbers(2)) AS QBit(Float32, 2));
SELECT CAST((SELECT groupArray(toFloat32(number + 0.1)) FROM numbers(2)) AS QBit(Float32, 2));
SELECT * FROM format('Values', 'qbit QBit(Float32, 2)', '(array(0.1,1.1))');


SELECT 'Test Array → QBit CAST AS: BFloat16';
SELECT CAST((SELECT groupArray(toBFloat16(number + 0.1)) FROM numbers(2)) AS QBit(BFloat16, 2));
SELECT CAST((SELECT groupArray(toBFloat16(number + 0.1)) FROM numbers(2)) AS QBit(BFloat16, 2));
SELECT CAST((SELECT groupArray(toBFloat16(number + 0.1)) FROM numbers(2)) AS QBit(BFloat16, 2));
SELECT * FROM format('Values', 'qbit QBit(BFloat16, 2)', '(array(0.1,1.1))');

SELECT * FROM format('Values', 'qbit QBit(BFloat16, 3)', '(tuple([1,2,3]::QBit(BFloat16, 3).1,
                                                                 [1,2,3]::QBit(BFloat16, 3).2,
                                                                 [1,2,3]::QBit(BFloat16, 3).3,
                                                                 [1,2,3]::QBit(BFloat16, 3).4,
                                                                 [1,2,3]::QBit(BFloat16, 3).5,
                                                                 [1,2,3]::QBit(BFloat16, 3).6,
                                                                 [1,2,3]::QBit(BFloat16, 3).7,
                                                                 [1,2,3]::QBit(BFloat16, 3).8,
                                                                 [1,2,3]::QBit(BFloat16, 3).9,
                                                                 [1,2,3]::QBit(BFloat16, 3).10,
                                                                 [1,2,3]::QBit(BFloat16, 3).11,
                                                                 [1,2,3]::QBit(BFloat16, 3).12,
                                                                 [1,2,3]::QBit(BFloat16, 3).13,
                                                                 [1,2,3]::QBit(BFloat16, 3).14,
                                                                 [1,2,3]::QBit(BFloat16, 3).15,
                                                                 [1,2,3]::QBit(BFloat16, 3).16))');

SELECT * FROM format('Values', 'qbit QBit(BFloat16, 3)', '(tuple([1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).1,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).2,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).3,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).4,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).5,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).6,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).7,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).8,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).9,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).10,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).11,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).12,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).13,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).14,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).15,
                                                                 [1,2,3,4,5,6,7,8,9]::QBit(BFloat16, 9).16))');  -- { serverError TYPE_MISMATCH }

SELECT * FROM format('Values', 'qbit QBit(BFloat16, 9)', '(tuple([1,2,3]::QBit(BFloat16, 3).1))'); -- { serverError TYPE_MISMATCH }


SELECT 'Test with and without analyzer / constant QBit';
SELECT L2DistanceTransposed([1,2,3]::QBit(Float64, 3), [1,2,3]::Array(Float64), 3) settings enable_analyzer=0;
SELECT L2DistanceTransposed([1,2,3]::QBit(Float64, 3), [1,2,3]::Array(Float64), 3) settings enable_analyzer=1;
SELECT L2DistanceTransposed(materialize([1,2,3]::QBit(Float64, 3)), [1,2,3]::Array(Float64), 3) settings enable_analyzer=0;
SELECT L2DistanceTransposed(materialize([1,2,3]::QBit(Float64, 3)), [1,2,3]::Array(Float64), 3) settings enable_analyzer=1;

SELECT 'Difficult tests';
SELECT bin(CAST([0.1, 0.2], 'QBit(Float64, 2)').1) AS tuple_result;
SELECT bin(CAST([0.1, 0.2], 'QBit(Float64, 2)').2) AS tuple_result;
SELECT bin(CAST([0.1, 0.2], 'QBit(Float64, 2)').3) AS tuple_result;
