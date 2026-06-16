SELECT groupArrayMovingAvg ( toInt64 ( 0 ) ) ( toDecimal32 ( 1 , 1 ) ); -- { serverError BAD_ARGUMENTS }

