SYSTEM SET MERGE POOL SIZE 32;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundMergesAndMutationsPool';
SYSTEM SET MERGE POOL SIZE 16;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundMergesAndMutationsPool';

SYSTEM SET MOVE POOL SIZE 16;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundMovePool';
SYSTEM SET MOVE POOL SIZE 8;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundMovePool';

SYSTEM SET FETCH POOL SIZE 16;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundFetchesPool';
SYSTEM SET FETCH POOL SIZE 8;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundFetchesPool';


SYSTEM SET COMMON POOL SIZE 16;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundCommonPool';
SYSTEM SET COMMON POOL SIZE 8;
SELECT value FROM system.metrics WHERE metric  = 'BackgroundCommonPool';
