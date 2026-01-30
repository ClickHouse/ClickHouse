DROP TABLE IF EXISTS 03821_insert_profile_events;


CREATE TABLE 03821_insert_profile_events
(
    col1 int
) ENGINE MergeTree() 
ORDER BY col1 
;

//async_insert=0  

insert into 03821_insert_profile_events values(1);
insert into 03821_insert_profile_events values(2);

//async_insert=1

insert into 03821_insert_profile_events settings  async_insert=1,     wait_for_async_insert=1,
    async_insert_busy_timeout_ms=1000  values(3),(4);

insert into 03821_insert_profile_events settings  async_insert=1,     wait_for_async_insert=1,
    async_insert_busy_timeout_ms=1000  values(5),(6);


SELECT
    name,
    value
FROM  system.events
WHERE event IN ('InsertQuery', 'AsyncInsertQuery')
ORDER BY name ASC
FORMAT TSVRaw 
;


