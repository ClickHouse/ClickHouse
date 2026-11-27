SELECT toDateTime64('-123', 3, 'UTC');    -- Allowed: no year starts with '-'
SELECT toDateTime64('23.9', 3, 'UTC');    -- Allowed: no year has a dot in notation
SELECT toDateTime64('-23.9', 3, 'UTC');   -- Allowed

SELECT toDateTime64OrNull('0', 3, 'UTC');
SELECT cast('0' as Nullable(DateTime64(3, 'UTC')));

SELECT toDateTime64('1234', 3, 'UTC');      -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64('0', 3, 'UTC');         -- { serverError CANNOT_PARSE_DATETIME }
SELECT cast('0' as DateTime64(3, 'UTC'));   -- { serverError CANNOT_PARSE_DATETIME }
