SET send_logs_level = 'none';

SELECT globalNotIn(['"wh'], [NULL]);
SELECT globalIn([''], [NULL]);
SELECT notIn([['']], [[NULL]]);
