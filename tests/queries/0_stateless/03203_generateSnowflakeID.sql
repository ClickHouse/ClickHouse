-- Test generateSnowflakeID without machine_id
SELECT generateSnowflakeID() AS id1;

-- Test generateSnowflakeID with machine_id
SELECT generateSnowflakeID(123) AS id2;

-- Test generateSnowflakeID with different machine_id
SELECT generateSnowflakeID(456) AS id3;

-- Test generateSnowflakeID with zero machine_id
SELECT generateSnowflakeID(0) AS id4;
