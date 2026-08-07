SELECT formatRow('RawBLOB', [[[33]], []]); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', [[[]], []]); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', [[[[[[[0x48, 0x65, 0x6c, 0x6c, 0x6f]]]]]], []]); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', []::Array(Array(Nothing))); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', [[], [['Hello']]]); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', [[['World']], []]); -- { serverError NOT_IMPLEMENTED }
SELECT formatRow('RawBLOB', []::Array(String)); -- { serverError NOT_IMPLEMENTED }
