SELECT 1 SETTINGS max_execution_time=NaN; -- { clientError 72 }
SELECT 1 SETTINGS max_execution_time=Infinity; -- { clientError 72 };
SELECT 1 SETTINGS max_execution_time=-Infinity; -- { clientError 72 };

-- Ok values
SELECT 1 SETTINGS max_execution_time=-0.5;
SELECT 1 SETTINGS max_execution_time=5.5;
SELECT 1 SETTINGS max_execution_time=-1;
SELECT 1 SETTINGS max_execution_time=0.0;
SELECT 1 SETTINGS max_execution_time=-0.0;
SELECT 1 SETTINGS max_execution_time=10;
