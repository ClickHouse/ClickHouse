-- Test case insensitive yesterday() function
SELECT YESTERDAY() = yesterday();
SELECT Yesterday() = yesterday();
SELECT YesterDay() = yesterday();
SELECT yESterdAy() = yesterday();
