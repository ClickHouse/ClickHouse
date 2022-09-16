SELECT formatRow('RawBLOB', [[[33]], []]);
SELECT formatRow('RawBLOB', [[[]], []]);
SELECT formatRow('RawBLOB', [[[[[[[0x48, 0x65, 0x6c, 0x6c, 0x6f]]]]]], []]);
SELECT formatRow('RawBLOB', []::Array(Array(Nothing)));
SELECT formatRow('RawBLOB', [[], [['Hello']]]);
SELECT formatRow('RawBLOB', [[['World']], []]);
SELECT formatRow('RawBLOB', []::Array(String));
