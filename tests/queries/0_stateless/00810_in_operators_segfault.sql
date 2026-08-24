SET send_logs_level = 'fatal';

SELECT globalNotIn(['"wh'], [NULL]);
SELECT globalIn([''], [NULL]);
SELECT notIn([['']], [[NULL]]);
