-- Tests that formatDateTime correctly handles the case of variable-size formatter (e.g. %W aka. weekday 'Monday', 'Tuesday', etc.),
-- followed by a compound formatter (= formatter that print multiple components at once, e.g. %D aka. the American date '05/04/25')

SELECT formatDateTime(toDateTime('2025-05-04'), '%W %D');
SELECT formatDateTime(toDateTime('2025-05-04'), '%W %F');
SELECT formatDateTime(toDateTime('2025-05-04'), '%W %r');
SELECT formatDateTime(toDateTime('2025-05-04'), '%W %R');
SELECT formatDateTime(toDateTime('2025-05-04'), '%W %T');
