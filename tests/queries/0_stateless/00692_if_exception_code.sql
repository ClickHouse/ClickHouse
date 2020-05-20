SET send_logs_level = 'none';

SELECT if(); -- { serverError 42 }
SELECT if(1); -- { serverError 42 }
SELECT if(1, 1); -- { serverError 42 }
SELECT if(1, 1, 1);
