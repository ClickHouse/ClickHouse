SELECT today() = current_date();
SELECT today() = CURRENT_DATE();
SELECT today() = current_DATE();
SELECT today() = curdate();
SELECT today() = CURDATE();
SELECT today() = curDATE();

-- Test case insensitive today() function
SELECT TODAY() = today();
SELECT Today() = today();
SELECT ToDay() = today();
SELECT tOdAy() = today();
