SET send_logs_level = 'fatal';

SELECT if(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT if(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT if(1, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT if(1, 1, 1);
