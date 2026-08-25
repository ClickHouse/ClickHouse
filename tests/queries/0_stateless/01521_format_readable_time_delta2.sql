SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'years');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'months');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'days');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'hours');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'minutes');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'seconds');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'second'); -- { serverError BAD_ARGUMENTS }

SELECT formatReadableTimeDelta(-(60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 86400 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400));

SELECT formatReadableTimeDelta(1e100);
SELECT formatReadableTimeDelta(1e100, 'months');
SELECT formatReadableTimeDelta(1e100, 'days');
SELECT formatReadableTimeDelta(1e100, 'hours');
SELECT formatReadableTimeDelta(1e100, 'minutes');
SELECT formatReadableTimeDelta(1e100, 'seconds');

SELECT formatReadableTimeDelta(0x1000000000000000);

SELECT formatReadableTimeDelta(); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT formatReadableTimeDelta(1, 'years', 'seconds', 'extra'); -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
SELECT formatReadableTimeDelta('not_a_number'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT formatReadableTimeDelta(1, 123); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT formatReadableTimeDelta(1, 123, 'years'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT formatReadableTimeDelta(1, 'years', 456); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
