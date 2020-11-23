SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400));
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'years');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'months');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'days');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'hours');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'minutes');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'seconds');
SELECT formatReadableTimeDelta(-(1 + 60 + 3600 + 86400 + 30.5 * 86400 + 365 * 86400), 'second'); -- { serverError 36 }

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
