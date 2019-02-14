SET send_logs_level = 'none';

SELECT globalNotIn(['"wh'], [NULL]); -- { serverError 53 }
SELECT globalIn([''], [NULL]); -- { serverError 53 }
SELECT notIn([['']], [[NULL]]); -- { serverError 53 }
