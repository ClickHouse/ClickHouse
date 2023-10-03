DROP TABLE IF EXISTS events;

create table events (
    `organisation_id` UUID,
    `session_id` UUID,
    `id` UUID DEFAULT generateUUIDv4(),
    `timestamp` UInt64,
    `payload` String,
    `customer_id` UUID,
    `call_id` String,
    PROJECTION events_by_session_and_org
    (
        SELECT *
        ORDER BY
            organisation_id,
            session_id,
            timestamp
    ),
    PROJECTION events_by_session
    (
        SELECT *
        ORDER BY
            session_id,
            timestamp
    ),
    PROJECTION events_by_session_and_customer
    (
        SELECT *
        ORDER BY
            customer_id,
            session_id,
            timestamp
    ),
    PROJECTION events_by_call_id
    (
        SELECT *
        ORDER BY
            call_id,
            timestamp
    )) engine = MergeTree order by (organisation_id, session_id, timestamp) settings index_granularity = 3;

insert into events values (reinterpretAsUUID(0), reinterpretAsUUID(1), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(0), reinterpretAsUUID(1), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(1), reinterpretAsUUID(0), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(1), reinterpretAsUUID(0), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(3), reinterpretAsUUID(2), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(3), reinterpretAsUUID(2), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0));
insert into events values (reinterpretAsUUID(0), reinterpretAsUUID(1), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(0), reinterpretAsUUID(1), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(1), reinterpretAsUUID(0), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(1), reinterpretAsUUID(0), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(3), reinterpretAsUUID(2), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0)), (reinterpretAsUUID(3), reinterpretAsUUID(2), reinterpretAsUUID(0), toDateTime('2022-02-02', 'UTC'), toString(0), reinterpretAsUUID(0), toString(0));

set read_in_order_two_level_merge_threshold=1;
SELECT id, timestamp, payload FROM events WHERE (organisation_id = reinterpretAsUUID(1)) AND (session_id = reinterpretAsUUID(0)) ORDER BY timestamp, payload, id ASC;
