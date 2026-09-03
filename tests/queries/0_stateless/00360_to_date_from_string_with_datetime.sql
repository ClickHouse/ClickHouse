SELECT toDate('2016-08-02 12:34:19');
SELECT toDate(toString(toDateTime('2000-01-01 00:00:00') + number)) FROM system.numbers LIMIT 3;
