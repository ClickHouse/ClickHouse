-- Result does not make sense but UBSan report should not be triggered.
SELECT ignore(now() + INTERVAL 9223372036854775807 MONTH);
