-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/52844
SELECT windowFunnel(100000, 'strict_increase')(
    time,
    event_type = 'page_visit0',
    event_type = 'page_visit1',
    event_type = 'page_visit2',
    event_type = 'page_visit3')
FROM
(
    SELECT
        event.1 AS time,
        event.2 AS event_type
    FROM
    (
        SELECT arrayJoin([
            (1::UInt64, 'page_visit0'),
            (2::UInt64, 'page_visit1'),
            (2::UInt64, 'page_visit2'),
            (2::UInt64, 'page_visit3'),
            (3::UInt64, 'page_visit1'),
            (3::UInt64, 'page_visit2'),
            (3::UInt64, 'page_visit3'),
            (4::UInt64, 'page_visit1'),
            (4::UInt64, 'page_visit2'),
            (4::UInt64, 'page_visit3')]) AS event
    )
);

-- A later first event must still replace an earlier one to keep the window sliding.
SELECT windowFunnel(1, 'strict_increase')(time, event_type = 'a', event_type = 'b')
FROM
(
    SELECT
        event.1 AS time,
        event.2 AS event_type
    FROM
    (
        SELECT arrayJoin([(1::UInt64, 'a'), (2::UInt64, 'a'), (3::UInt64, 'b')]) AS event
    )
);

-- Updates at timestamp 6 must not hide the valid chain ending before timestamp 6.
SELECT windowFunnel(5, 'strict_increase')(time, event_type = 'a', event_type = 'b', event_type = 'c')
FROM
(
    SELECT
        event.1 AS time,
        event.2 AS event_type
    FROM
    (
        SELECT arrayJoin([
            (1::UInt64, 'a'),
            (2::UInt64, 'b'),
            (5::UInt64, 'a'),
            (6::UInt64, 'b'),
            (6::UInt64, 'c')]) AS event
    )
);

-- After the timestamp group is complete, the newer chain must become available.
SELECT windowFunnel(2, 'strict_increase')(time, event_type = 'a', event_type = 'b', event_type = 'c')
FROM
(
    SELECT
        event.1 AS time,
        event.2 AS event_type
    FROM
    (
        SELECT arrayJoin([
            (1::UInt64, 'a'),
            (2::UInt64, 'b'),
            (5::UInt64, 'a'),
            (6::UInt64, 'b'),
            (7::UInt64, 'c')]) AS event
    )
);
