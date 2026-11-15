SELECT 1 SETTINGS max_execution_time=NaN; -- { clientError CANNOT_PARSE_NUMBER }
SELECT 1 SETTINGS max_execution_time=Infinity; -- { clientError CANNOT_PARSE_NUMBER };
SELECT 1 SETTINGS max_execution_time=-Infinity; -- { clientError CANNOT_PARSE_NUMBER };
SELECT 1 SETTINGS max_execution_time=-0.5; -- { clientError BAD_ARGUMENTS }
SELECT 1 SETTINGS max_execution_time=-1; -- { clientError BAD_ARGUMENTS }

-- Ok values
SELECT 1 SETTINGS max_execution_time=5.5;
SELECT 1 SETTINGS max_execution_time=0.0;
SELECT 1 SETTINGS max_execution_time=-0.0;
SELECT 1 SETTINGS max_execution_time=10;
