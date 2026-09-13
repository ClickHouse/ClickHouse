SELECT number FROM numbers(3) LIMIT AFTER arrayJoin([number]) > 0; -- { serverError UNEXPECTED_EXPRESSION }
SELECT number FROM numbers(3) LIMIT UNTIL arrayJoin([number]) > 0; -- { serverError UNEXPECTED_EXPRESSION }

SELECT number FROM numbers(3) LIMIT AFTER arrayJoin([number]) > 0 SETTINGS enable_analyzer = 0; -- { serverError UNEXPECTED_EXPRESSION }
SELECT number FROM numbers(3) LIMIT UNTIL arrayJoin([number]) > 0 SETTINGS enable_analyzer = 0; -- { serverError UNEXPECTED_EXPRESSION }
