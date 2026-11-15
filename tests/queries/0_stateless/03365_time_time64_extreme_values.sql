SET use_legacy_to_time = 0;

-- Within the acceptable range
SELECT toTime('999:59:59');
SELECT toTime64('999:59:59.999999999', 9);
SELECT toTime64('999:59:59.9999999999999', 9);
SELECT toTime('-999:59:59');
SELECT toTime64('-999:59:59.999999999', 9);
SELECT toTime64('-999:59:59.9999999999999', 9);
SELECT toTime('0:00:00');
SELECT toTime64('0:00:00.0', 9);
SELECT toTime64('0:00:00.0', 9);
SELECT toTime('-0:00:00');
SELECT toTime64('-0:00:00.0', 9);
SELECT toTime64('-0:00:00.0', 9);

-- Exceeds the acceptable range
SELECT toTime('999:99:99');
