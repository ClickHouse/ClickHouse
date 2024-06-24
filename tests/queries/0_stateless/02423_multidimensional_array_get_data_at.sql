SELECT formatRow('RawBLOB', [[[33]], []]); -- { serverError 48 }
SELECT formatRow('RawBLOB', [[[]], []]); -- { serverError 48 }
SELECT formatRow('RawBLOB', [[[[[[[0x48, 0x65, 0x6c, 0x6c, 0x6f]]]]]], []]); -- { serverError 48 }
SELECT formatRow('RawBLOB', []::Array(Array(Nothing))); -- { serverError 48 }
SELECT formatRow('RawBLOB', [[], [['Hello']]]); -- { serverError 48 }
SELECT formatRow('RawBLOB', [[['World']], []]); -- { serverError 48 }
SELECT formatRow('RawBLOB', []::Array(String)); -- { serverError 48 }
