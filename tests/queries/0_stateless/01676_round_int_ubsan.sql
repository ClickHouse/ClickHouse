-- Overflow during integer rounding is implementation specific behaviour.
-- This test allows to be aware if the impkementation changes.
-- Changing the implementation specific behaviour is Ok.
-- and should not be treat as incompatibility (simply update test result then).

SELECT round(-9223372036854775808, -2);
