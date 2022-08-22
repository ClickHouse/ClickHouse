select transform('a', ['a', 'b'], [toDateTime64(1, 3, 'UTC'), toDateTime64(2, 3, 'UTC')], toDateTime64(0, 3, 'UTC'));
select transform(2, [1, 2], [toDateTime64(1, 3, 'UTC'), toDateTime64(2, 3, 'UTC')], toDateTime64(0, 3, 'UTC'));
select transform(null, [1, 2], [toDateTime64(1, 3, 'UTC'), toDateTime64(2, 3, 'UTC')], toDateTime64(0, 3, 'UTC'));
