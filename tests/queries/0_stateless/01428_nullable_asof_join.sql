SET join_use_nulls = 1;

select 'left asof using';

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, toNullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt;-- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, toNullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt;-- { serverError 48 }

select 'left asof on';

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, toNullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;-- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF LEFT JOIN (SELECT 1 as pk, toNullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;-- { serverError 48 }

select 'asof using';

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
USING(pk, dt)
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, toNullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt;-- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, toNullable(0) as dt) b
USING(pk, dt)
ORDER BY a.dt;-- { serverError 48 }

select 'asof on';

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, 2 as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toUInt8(number) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, toNullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;-- { serverError 48 }

SELECT a.pk, b.pk, a.dt, b.dt, toTypeName(a.pk), toTypeName(b.pk), toTypeName(materialize(a.dt)), toTypeName(materialize(b.dt))
FROM (SELECT toUInt8(number) > 0 as pk, toNullable(toUInt8(number)) as dt FROM numbers(3)) a
ASOF JOIN (SELECT 1 as pk, toNullable(0) as dt) b
ON a.pk = b.pk AND a.dt >= b.dt
ORDER BY a.dt;-- { serverError 48 }
